package sync

import (
	"context"

	// nolint
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/docker/go-units"
	"github.com/metal-stack/metal-image-cache-sync/cmd/internal/metrics"
	"github.com/metal-stack/metal-image-cache-sync/pkg/api"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type Syncer struct {
	logger         *zap.SugaredLogger
	fs             afero.Fs
	tmpPath        string
	s3             *s3manager.Downloader
	stop           context.Context
	dry            bool
	imageCollector *metrics.ImageCollector
	httpClient     *http.Client
}

func NewSyncer(logger *zap.SugaredLogger, fs afero.Fs, s3 *s3manager.Downloader, config *api.Config, collector *metrics.ImageCollector, stop context.Context) (*Syncer, error) {
	err := fs.MkdirAll(config.GetImageRootPath(), 0755)
	if err != nil {
		return nil, errors.Wrap(err, "error creating image subdirectory in cache root")
	}
	err = fs.MkdirAll(config.GetKernelRootPath(), 0755)
	if err != nil {
		return nil, errors.Wrap(err, "error creating kernel subdirectory in cache root")
	}
	err = fs.MkdirAll(config.GetBootImageRootPath(), 0755)
	if err != nil {
		return nil, errors.Wrap(err, "error creating boot image subdirectory in cache root")
	}

	return &Syncer{
		logger:         logger,
		fs:             fs,
		tmpPath:        config.GetTmpDownloadPath(),
		s3:             s3,
		stop:           stop,
		httpClient:     http.DefaultClient,
		dry:            config.DryRun,
		imageCollector: collector,
	}, nil
}

func (s *Syncer) Sync(rootPath string, entitiesToSync api.CacheEntities) error {
	current, err := currentFileIndex(s.fs, rootPath)
	if err != nil {
		return errors.Wrap(err, "error creating file index")
	}

	remove, keep, add, err := s.defineDiff(rootPath, current, entitiesToSync)
	if err != nil {
		return errors.Wrap(err, "error creating cache diff")
	}

	s.printSyncPlan(remove, keep, add)

	if s.dry {
		s.logger.Infow("dry run: not downloading or deleting files")
		return nil
	}

	for _, e := range remove {
		err := s.remove(rootPath, e)
		if err != nil {
			return fmt.Errorf("error deleting cached file, retrying in next sync schedule: %w", err)
		}
	}

	for _, e := range add {
		err := s.download(rootPath, e)
		if err != nil {
			return fmt.Errorf("error downloading file, retrying in next sync schedule: %w", err)
		}
	}

	err = cleanEmptyDirs(s.fs, rootPath)
	if err != nil {
		return errors.Wrap(err, "error cleaning up empty directories")
	}

	return nil
}

func currentFileIndex(fs afero.Fs, rootPath string) (api.CacheEntities, error) {
	var result api.CacheEntities
	err := afero.Walk(fs, rootPath, func(path string, info os.FileInfo, innerErr error) error {
		if innerErr != nil {
			return errors.Wrap(innerErr, fmt.Sprintf("error while walking through root path %s", rootPath))
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".md5") {
			return nil
		}

		result = append(result, api.LocalFile{
			Name:    info.Name(),
			SubPath: path[len(rootPath)+1:],
			Size:    info.Size(),
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Syncer) defineDiff(rootPath string, currentEntities api.CacheEntities, wantEntities api.CacheEntities) (remove api.CacheEntities, keep api.CacheEntities, add api.CacheEntities, err error) {
	// define entities to add
	for _, wantEntity := range wantEntities {
		var existing api.CacheEntity
		for _, entityOnDisk := range currentEntities {
			if entityOnDisk.GetSubPath() == wantEntity.GetSubPath() {
				existing = entityOnDisk
				break
			}
		}

		if existing == nil {
			add = append(add, wantEntity)
			continue
		}

		if !wantEntity.HasMD5() {
			keep = append(keep, wantEntity)
			continue
		}

		expected, err := wantEntity.DownloadMD5(s.stop, nil, s.httpClient, s.s3)
		if err != nil {
			s.logger.Errorw("error downloading checksum", "error", err)
			continue
		}

		hash, err := s.fileMD5(strings.Join([]string{rootPath, existing.GetSubPath()}, string(os.PathSeparator)))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "error calculating hash sum of local file")
		}

		if hash != expected {
			s.logger.Infow("found image with invalid hash sum, schedule new download")
			add = append(add, wantEntity)
		} else {
			keep = append(keep, wantEntity)
		}
	}

	// define entities to remove
	for _, e := range currentEntities {
		found := false
		for _, wantEntity := range wantEntities {
			if e.GetSubPath() == wantEntity.GetSubPath() {
				found = true
			}
		}

		if !found {
			remove = append(remove, e)
		}
	}

	return remove, keep, add, err
}

func (s *Syncer) fileMD5(filePath string) (string, error) {
	file, err := s.fs.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := md5.New() // nolint
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (s *Syncer) download(rootPath string, e api.CacheEntity) error {
	tmpTargetPath := strings.Join([]string{s.tmpPath, "tmp"}, string(os.PathSeparator))
	targetPath := strings.Join([]string{rootPath, e.GetSubPath()}, string(os.PathSeparator))
	md5TargetPath := strings.Join([]string{rootPath, e.GetSubPath() + ".md5"}, string(os.PathSeparator))

	_ = s.fs.Remove(tmpTargetPath)
	_ = s.fs.Remove(targetPath)
	_ = s.fs.Remove(md5TargetPath)

	err := s.fs.MkdirAll(path.Dir(tmpTargetPath), 0755)
	if err != nil {
		return errors.Wrap(err, "error creating tmp download path in cache root")
	}

	err = s.fs.MkdirAll(path.Dir(targetPath), 0755)
	if err != nil {
		return errors.Wrap(err, "error creating path in cache root")
	}

	f, err := s.fs.Create(tmpTargetPath)
	if err != nil {
		return fmt.Errorf("error opening file path %s: %w", targetPath, err)
	}
	defer f.Close()

	s.logger.Infow("downloading file", "id", e.GetName(), "key", e.GetSubPath(), "size", e.GetSize(), "to", tmpTargetPath)
	n, err := e.Download(s.stop, f, s.httpClient, s.s3)
	if err != nil {
		return err
	}
	defer func() {
		_ = s.fs.Remove(tmpTargetPath)
	}()

	switch ent := e.(type) {
	case api.OS:
		s.imageCollector.AddSyncDownloadImageBytes(n)
		s.imageCollector.IncrementSyncDownloadImageCount()
	case api.BootImage:
	case api.Kernel:
	case api.LocalFile:
	default:
		s.logger.Errorw("unexpected entity type for metrics collection: %v", ent)
	}

	err = s.fs.Rename(tmpTargetPath, targetPath)
	if err != nil {
		return errors.Wrap(err, "error moving downloaded file to final destination")
	}

	if !e.HasMD5() {
		return nil
	}

	f, err = s.fs.Create(md5TargetPath)
	if err != nil {
		return fmt.Errorf("error opening file path %s: %w", md5TargetPath, err)
	}
	defer f.Close()

	s.logger.Infow("downloading md5 checksum", "id", e.GetName(), "key", e.GetSubPath(), "to", md5TargetPath)
	_, err = e.DownloadMD5(s.stop, &f, s.httpClient, s.s3)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) remove(rootPath string, e api.CacheEntity) error {
	path := strings.Join([]string{rootPath, e.GetSubPath()}, string(os.PathSeparator))
	s.logger.Infow("removing file from disk", "path", e.GetSubPath(), "id", e.GetName())
	err := s.fs.Remove(path)
	if err != nil {
		s.logger.Errorw("error deleting file", "error", err)
		return err
	}
	exists, err := afero.Exists(s.fs, path+".md5")
	if err != nil {
		s.logger.Errorw("error checking whether md5 exists", "error", err)
	} else if exists {
		err = s.fs.Remove(path + ".md5")
		if err != nil {
			s.logger.Errorw("error deleting os image md5 file", "error", err)
			return err
		}
	}
	return nil
}

func (s *Syncer) printSyncPlan(remove api.CacheEntities, keep []api.CacheEntity, add []api.CacheEntity) {
	cacheSize := int64(0)
	data := [][]string{}
	for _, e := range remove {
		data = append(data, []string{"", e.GetSubPath(), units.HumanSize(float64(e.GetSize())), "delete"})
	}
	for _, e := range keep {
		cacheSize += e.GetSize()
		data = append(data, []string{e.GetName(), e.GetSubPath(), units.HumanSize(float64(e.GetSize())), "keep"})
	}
	for _, e := range add {
		cacheSize += e.GetSize()
		data = append(data, []string{e.GetName(), e.GetSubPath(), units.HumanSize(float64(e.GetSize())), "download"})
	}

	s.logger.Infow("sync plan", "amount", len(keep)+len(add), "cache-size-after-sync", units.BytesSize(float64(cacheSize)))
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Path", "Size", "Action"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}

func cleanEmptyDirs(fs afero.Fs, rootPath string) error {
	files, err := afero.ReadDir(fs, rootPath)
	if err != nil {
		return err
	}

	for _, info := range files {
		if !info.IsDir() {
			continue
		}

		err = recurseCleanEmptyDirs(fs, path.Join(rootPath, info.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}

func recurseCleanEmptyDirs(fs afero.Fs, p string) error {
	files, err := afero.ReadDir(fs, p)
	if err != nil {
		return err
	}

	for _, info := range files {
		if !info.IsDir() {
			continue
		}

		nested := path.Join(p, info.Name())
		err = recurseCleanEmptyDirs(fs, nested)
		if err != nil {
			return err
		}
	}

	// re-read files because directories could delete themselves in first loop
	files, err = afero.ReadDir(fs, p)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		err = fs.Remove(p)
		if err != nil {
			return err
		}
	}

	return nil
}
