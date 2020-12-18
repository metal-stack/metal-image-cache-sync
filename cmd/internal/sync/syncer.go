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
	"github.com/aws/aws-sdk-go/aws"
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
	logger     *zap.SugaredLogger
	fs         afero.Fs
	rootPath   string
	s3         *s3manager.Downloader
	bucketName string
	stop       context.Context
	dry        bool
	collector  *metrics.Collector
}

func NewSyncer(logger *zap.SugaredLogger, fs afero.Fs, s3 *s3manager.Downloader, config *api.Config, collector *metrics.Collector, stop context.Context) (*Syncer, error) {
	absRoot, err := filepath.Abs(config.ImageCacheRootPath)
	if err != nil {
		return nil, err
	}
	return &Syncer{
		logger:     logger,
		fs:         fs,
		rootPath:   absRoot,
		s3:         s3,
		bucketName: config.ImageBucket,
		stop:       stop,
		dry:        config.DryRun,
		collector:  collector,
	}, nil
}

func (s *Syncer) Sync(imagesToSync []api.OS) error {
	currentImages, err := s.currentImageIndex()
	if err != nil {
		return errors.Wrap(err, "error creating image index")
	}

	remove, keep, add, err := s.defineImageDiff(currentImages, imagesToSync)
	if err != nil {
		return errors.Wrap(err, "error creating image diff")
	}

	s.printSyncPlan(remove, keep, add)

	if s.dry {
		s.logger.Infow("dry run: not downloading or deleting images")
		return nil
	}

	for _, image := range remove {
		err := s.remove(image)
		if err != nil {
			return fmt.Errorf("error deleting os image, retrying in next sync schedule: %v", err)
		}
	}

	for _, image := range add {
		err := s.download(s.stop, image)
		if err != nil {
			return fmt.Errorf("error downloading os image, retrying in next sync schedule: %v", err)
		}
	}

	return nil
}

func (s *Syncer) currentImageIndex() ([]api.OS, error) {
	var result []api.OS
	err := afero.Walk(s.fs, s.rootPath, func(p string, info os.FileInfo, innerErr error) error {
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
			BucketKey: p[len(s.rootPath)+1:],
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

func (s *Syncer) defineImageDiff(currentImages []api.OS, wantImages []api.OS) (remove []api.OS, keep []api.OS, add []api.OS, err error) {
	// define images to add
	for _, wantImage := range wantImages {
		var existing *api.OS
		for _, imageOnDisk := range currentImages {
			if imageOnDisk.BucketKey == wantImage.BucketKey {
				existing = &imageOnDisk
				break
			}
		}

		if existing == nil {
			add = append(add, wantImage)
			continue
		}

		buff := &aws.WriteAtBuffer{}
		_, err = s.s3.DownloadWithContext(s.stop, buff, &s3.GetObjectInput{
			Bucket: &s.bucketName,
			Key:    wantImage.MD5Ref.Key,
		})
		if err != nil {
			s.logger.Errorw("error downloading checksum of image", "key", wantImage.BucketKey, "error", err)
			continue
		}

		hash, err := s.fileMD5(strings.Join([]string{s.rootPath, existing.BucketKey}, string(os.PathSeparator)))
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "error calculating hash sum of local file")
		}

		expected := strings.Split(string(buff.Bytes()), " ")[0]

		if hash != expected {
			s.logger.Infow("found image with invalid hash sum, schedule new download")
			add = append(add, wantImage)
		} else {
			keep = append(keep, wantImage)
		}
	}

	// define images to remove
	for _, image := range currentImages {
		found := false
		for _, wantImage := range wantImages {
			if image.BucketKey == wantImage.BucketKey {
				found = true
			}
		}

		if !found {
			remove = append(remove, image)
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

func (s *Syncer) download(ctx context.Context, image api.OS) error {
	targetPath := strings.Join([]string{s.rootPath, image.BucketKey}, string(os.PathSeparator))
	md5TargetPath := strings.Join([]string{s.rootPath, image.BucketKey + ".md5"}, string(os.PathSeparator))

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

	s.logger.Infow("downloading os image", "image", image.BucketKey, "to", targetPath)
	n, err := s.s3.DownloadWithContext(ctx, f, &s3.GetObjectInput{
		Bucket: &s.bucketName,
		Key:    &image.BucketKey,
	})
	if err != nil {
		return errors.Wrap(err, "image download error")
	}
	s.collector.AddSyncDownloadImageBytes(n)
	s.collector.IncrementSyncDownloadImageCount()

	f, err = s.fs.Create(md5TargetPath)
	if err != nil {
		return fmt.Errorf("error opening file path %s: %v", md5TargetPath, err)
	}
	defer f.Close()

	s.logger.Infow("downloading os image md5 checksum", "image", image.BucketKey, "to", md5TargetPath)
	_, err = s.s3.DownloadWithContext(ctx, f, &s3.GetObjectInput{
		Bucket: &s.bucketName,
		Key:    image.MD5Ref.Key,
	})
	if err != nil {
		return errors.Wrap(err, "image md5 download error")
	}

	return nil
}

func (s *Syncer) remove(image api.OS) error {
	path := strings.Join([]string{s.rootPath, image.BucketKey}, string(os.PathSeparator))
	s.logger.Infow("removing image from disk", "image", image.BucketKey)
	err := s.fs.Remove(path)
	if err != nil {
		s.logger.Errorw("error deleting os image", "error", err)
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

func (s *Syncer) printSyncPlan(remove []api.OS, keep []api.OS, add []api.OS) {
	cacheSize := int64(0)
	data := [][]string{}
	for _, img := range remove {
		data = append(data, []string{"", img.BucketKey, units.HumanSize(float64(*img.ImageRef.Size)), "delete"})
	}
	for _, img := range keep {
		cacheSize += *img.ImageRef.Size
		data = append(data, []string{*img.ApiRef.ID, img.BucketKey, units.HumanSize(float64(*img.ImageRef.Size)), "keep"})
	}
	for _, img := range add {
		cacheSize += *img.ImageRef.Size
		data = append(data, []string{*img.ApiRef.ID, img.BucketKey, units.HumanSize(float64(*img.ImageRef.Size)), "download"})
	}

	s.logger.Infow("sync plan", "amount", len(keep)+len(add), "cache-size-after-sync", units.BytesSize(float64(cacheSize)))
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Path", "Size", "Action"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()
}
