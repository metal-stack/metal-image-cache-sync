package sync

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/aws/aws-sdk-go/service/s3"
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
	logger        *zap.SugaredLogger
	fs            afero.Fs
	imageRoot     string
	kernelRoot    string
	bootImageRoot string
	s3            *s3manager.Downloader
	bucketName    string
	stop          context.Context
	dry           bool
	collector     *metrics.Collector
}

func NewSyncer(logger *zap.SugaredLogger, fs afero.Fs, s3 *s3manager.Downloader, config *api.Config, collector *metrics.Collector, stop context.Context) (*Syncer, error) {
	absImageRoot, err := filepath.Abs(config.ImageCacheRootPath)
	if err != nil {
		return nil, err
	}
	absKernelRoot, err := filepath.Abs(config.KernelCacheRootPath)
	if err != nil {
		return nil, err
	}
	absBootImageRoot, err := filepath.Abs(config.BootImageCacheRootPath)
	if err != nil {
		return nil, err
	}
	return &Syncer{
		logger:        logger,
		fs:            fs,
		imageRoot:     absImageRoot,
		kernelRoot:    absKernelRoot,
		bootImageRoot: absBootImageRoot,
		s3:            s3,
		bucketName:    config.ImageBucket,
		stop:          stop,
		dry:           config.DryRun,
		collector:     collector,
	}, nil
}

func (s *Syncer) SyncImages(imagesToSync []api.OS) error {
	current, err := s.currentImageIndex()
	if err != nil {
		return errors.Wrap(err, "error creating image index")
	}

	var toSync api.CacheEntities
	for _, i := range imagesToSync {
		toSync = append(toSync, i)
	}

	err = s.sync(s.imageRoot, current, toSync)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) SyncKernels(kernelsToSync []api.Kernel) error {
	current, err := s.currentKernelIndex()
	if err != nil {
		return errors.Wrap(err, "error creating image index")
	}

	var toSync api.CacheEntities
	for _, k := range kernelsToSync {
		toSync = append(toSync, k)
	}

	err = s.sync(s.kernelRoot, current, toSync)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) SyncBootImages(imagesToSync []api.BootImage) error {
	current, err := s.currentBootImageIndex()
	if err != nil {
		return errors.Wrap(err, "error creating image index")
	}

	var toSync api.CacheEntities
	for _, i := range imagesToSync {
		toSync = append(toSync, i)
	}

	err = s.sync(s.bootImageRoot, current, toSync)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) sync(rootPath string, current api.CacheEntities, toSync api.CacheEntities) error {
	remove, keep, add, err := s.defineDiff(rootPath, current, toSync)
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
			return fmt.Errorf("error deleting cached file, retrying in next sync schedule: %v", err)
		}
	}

	for _, e := range add {
		err := s.download(s.stop, rootPath, e)
		if err != nil {
			return fmt.Errorf("error downloading file, retrying in next sync schedule: %v", err)
		}
	}

	return nil
}

func (s *Syncer) currentImageIndex() (api.CacheEntities, error) {
	var result api.CacheEntities
	err := afero.Walk(s.fs, s.imageRoot, func(p string, info os.FileInfo, innerErr error) error {
		if innerErr != nil {
			return errors.Wrap(innerErr, "error while walking through cache root")
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(p, ".md5") {
			return nil
		}

		size := info.Size()

		result = append(result, api.OS{
			BucketKey: p[len(s.imageRoot)+1:],
			Version:   &semver.Version{},
			ImageRef: s3.Object{
				Size: &size,
			},
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Syncer) currentKernelIndex() (api.CacheEntities, error) {
	var result api.CacheEntities
	err := afero.Walk(s.fs, s.kernelRoot, func(p string, info os.FileInfo, innerErr error) error {
		if innerErr != nil {
			return errors.Wrap(innerErr, "error while walking through cache root")
		}

		if info.IsDir() {
			return nil
		}

		size := info.Size()

		result = append(result, api.Kernel{
			Key:  p[len(s.kernelRoot)+1:],
			Size: size,
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Syncer) currentBootImageIndex() (api.CacheEntities, error) {
	var result api.CacheEntities
	err := afero.Walk(s.fs, s.bootImageRoot, func(p string, info os.FileInfo, innerErr error) error {
		if innerErr != nil {
			return errors.Wrap(innerErr, "error while walking through cache root")
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(p, ".md5") {
			return nil
		}

		size := info.Size()

		result = append(result, api.BootImage{
			Key:  p[len(s.bootImageRoot)+1:],
			Size: size,
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
			if entityOnDisk.GetPath() == wantEntity.GetPath() {
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

		expected, err := wantEntity.DownloadMD5(s.stop, nil, s.s3)
		if err != nil {
			s.logger.Errorw("error downloading checksum", "error", err)
			continue
		}

		hash, err := s.fileMD5(strings.Join([]string{rootPath, existing.GetPath()}, string(os.PathSeparator)))
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
			if e.GetPath() == wantEntity.GetPath() {
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
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (s *Syncer) download(ctx context.Context, rootPath string, e api.CacheEntity) error {
	targetPath := strings.Join([]string{rootPath, e.GetPath()}, string(os.PathSeparator))
	md5TargetPath := strings.Join([]string{rootPath, e.GetPath() + ".md5"}, string(os.PathSeparator))

	_ = s.fs.Remove(targetPath)
	_ = s.fs.Remove(md5TargetPath)

	err := s.fs.MkdirAll(path.Dir(targetPath), 0755)
	if err != nil {
		return errors.Wrap(err, "error creating path in cache root")
	}

	f, err := s.fs.Create(targetPath)
	if err != nil {
		return fmt.Errorf("error opening file path %s: %v", targetPath, err)
	}
	defer f.Close()

	s.logger.Infow("downloading file", "id", e.GetName(), "key", e.GetPath(), "size", e.GetSize(), "to", targetPath)
	n, err := e.Download(s.stop, f, s.s3)
	if err != nil {
		return err
	}

	s.collector.AddSyncDownloadImageBytes(n)
	s.collector.IncrementSyncDownloadImageCount()

	if !e.HasMD5() {
		return nil
	}

	f, err = s.fs.Create(md5TargetPath)
	if err != nil {
		return fmt.Errorf("error opening file path %s: %v", md5TargetPath, err)
	}
	defer f.Close()

	s.logger.Infow("downloading md5 checksum", "id", e.GetName(), "key", e.GetPath(), "to", md5TargetPath)
	e.DownloadMD5(s.stop, &f, s.s3)
	if err != nil {
		return err
	}

	return nil
}

func (s *Syncer) remove(rootPath string, e api.CacheEntity) error {
	path := strings.Join([]string{rootPath, e.GetPath()}, string(os.PathSeparator))
	s.logger.Infow("removing file from disk", "path", e.GetPath(), "id", e.GetName())
	err := s.fs.Remove(path)
	if err != nil {
		s.logger.Errorw("error deleting file", "error", err)
	}
	exists, err := afero.Exists(s.fs, path+".md5")
	if err != nil {
		s.logger.Errorw("error checking whether md5 exists", "error", err)
	} else if exists {
		err = s.fs.Remove(path + ".md5")
		if err != nil {
			s.logger.Errorw("error deleting os image md5 file", "error", err)
		}
	}
	return nil
}

func (s *Syncer) printSyncPlan(remove api.CacheEntities, keep []api.CacheEntity, add []api.CacheEntity) {
	cacheSize := int64(0)
	data := [][]string{}
	for _, e := range remove {
		data = append(data, []string{"", e.GetPath(), units.HumanSize(float64(e.GetSize())), "delete"})
	}
	for _, e := range keep {
		cacheSize += e.GetSize()
		data = append(data, []string{e.GetName(), e.GetPath(), units.HumanSize(float64(e.GetSize())), "keep"})
	}
	for _, e := range add {
		cacheSize += e.GetSize()
		data = append(data, []string{e.GetName(), e.GetPath(), units.HumanSize(float64(e.GetSize())), "download"})
	}

	s.logger.Infow("sync plan", "amount", len(keep)+len(add), "cache-size-after-sync", units.BytesSize(float64(cacheSize)))
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Path", "Size", "Action"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}
