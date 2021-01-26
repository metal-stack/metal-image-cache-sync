package synclister

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	metalgo "github.com/metal-stack/metal-go"
	"github.com/metal-stack/metal-image-cache-sync/cmd/internal/metrics"
	"github.com/metal-stack/metal-image-cache-sync/pkg/api"
	"github.com/metal-stack/metal-image-cache-sync/pkg/utils"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type SyncLister struct {
	logger       *zap.SugaredLogger
	driver       *metalgo.Driver
	config       *api.Config
	s3           *s3.S3
	excludePaths []string
	collector    *metrics.Collector
}

func NewSyncLister(logger *zap.SugaredLogger, driver *metalgo.Driver, s3 *s3.S3, collector *metrics.Collector, config *api.Config) *SyncLister {
	return &SyncLister{
		logger:       logger,
		driver:       driver,
		config:       config,
		s3:           s3,
		excludePaths: config.ExcludePaths,
		collector:    collector,
	}
}

func (s *SyncLister) DetermineSyncList() ([]api.OS, error) {
	s3Images, err := s.retrieveImagesFromS3()
	if err != nil {
		return nil, errors.Wrap(err, "error listing images in s3")
	}

	resp, err := s.driver.ImageList()
	if err != nil {
		return nil, errors.Wrap(err, "error listing images")
	}

	s.collector.SetMetalAPIImageCount(len(resp.Image))

	images := api.OSImagesByOS{}
	for _, img := range resp.Image {
		skip := false
		for _, exclude := range s.excludePaths {
			if strings.Contains(img.URL, exclude) {
				skip = true
				break
			}
		}

		if skip {
			s.logger.Debugw("skipping image with exclude URL", "id", *img.ID)
			continue
		}

		if img.ExpirationDate != nil {
			if time.Since(time.Time(*img.ExpirationDate)) > 0 {
				s.logger.Debugw("not considering expired image, skipping", "id", *img.ID)
				continue
			}
		}

		os, ver, err := utils.GetOsAndSemver(*img.ID)
		if err != nil {
			s.logger.Errorw("could not extract os and version, skipping", "error", err)
			continue
		}

		versions, ok := images[os]
		if !ok {
			versions = api.OSImagesByVersion{}
		}

		majorMinor := fmt.Sprintf("%d.%d", ver.Major(), ver.Minor())
		imageVersions := versions[majorMinor]

		u, err := url.Parse(img.URL)
		if err != nil {
			s.logger.Errorw("image url is invalid, skipping", "error", err)
			continue
		}

		bucketKey := u.Path[1:]

		s3Image, ok := s3Images[bucketKey]
		if !ok {
			s.logger.Errorw("image is not contained in global image store, skipping", "path", u.Path, "id", *img.ID)
			continue
		}

		s3MD5, ok := s3Images[bucketKey+".md5"]
		if !ok {
			s.logger.Errorw("image md5 is not contained in global image store, skipping", "path", u.Path, "id", *img.ID)
			continue
		}

		imageVersions = append(imageVersions, api.OS{
			Name:      os,
			Version:   ver,
			ApiRef:    *img,
			BucketKey: bucketKey,
			ImageRef:  s3Image,
			MD5Ref:    s3MD5,
		})

		versions[majorMinor] = imageVersions
		images[os] = versions
	}

	var sizeCount int64
	var syncImages []api.OS
	for _, versions := range images {
		for _, images := range versions {
			sort.Slice(images, func(i, j int) bool {
				return images[i].Version.GreaterThan(images[j].Version)
			})
			amount := 0
			for _, img := range images {
				if s.config.MaxImagesPerName > 0 && amount >= s.config.MaxImagesPerName {
					break
				}
				amount += 1
				sizeCount += *img.ImageRef.Size
				syncImages = append(syncImages, img)
			}
		}
	}

	api.SortOSImagesByName(syncImages)

	for {
		if sizeCount < s.config.MaxCacheSize {
			break
		}

		syncImages, sizeCount, err = s.reduce(syncImages, sizeCount)
		if err != nil {
			s.logger.Warn("cannot reduce anymore images (all at minimum size), exceeding maximum cache size")
			break
		}
	}

	s.collector.SetUnsyncedImageCount(len(resp.Image) - len(syncImages))

	return syncImages, nil
}

func (s *SyncLister) reduce(images []api.OS, sizeCount int64) ([]api.OS, int64, error) {
	groups := map[string][]api.OS{}
	for _, img := range images {
		key := fmt.Sprintf("%s-%d.%d", img.Name, img.Version.Major(), img.Version.Minor())
		groups[key] = append(groups[key], img)
	}

	var biggestGroup string
	currentBiggest := 1
	var groupNames []string
	for g := range groups {
		groupNames = append(groupNames, g)
	}
	sort.Strings(groupNames)
	for _, name := range groupNames {
		amount := len(groups[name])
		if amount > s.config.MinImagesPerName && amount > currentBiggest {
			currentBiggest = amount
			biggestGroup = name
		}
	}

	if biggestGroup == "" {
		return images, sizeCount, fmt.Errorf("can not reduce any further")
	}

	groupImages := groups[biggestGroup]
	groups[biggestGroup] = append([]api.OS{}, groupImages[1:]...)

	newSize := sizeCount - *groupImages[0].ImageRef.Size

	var result []api.OS
	for _, imgs := range groups {
		result = append(result, imgs...)
	}

	api.SortOSImagesByName(result)

	return result, newSize, nil
}

func (s *SyncLister) retrieveImagesFromS3() (map[string]s3.Object, error) {
	res := map[string]s3.Object{}

	err := s.s3.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: &s.config.ImageBucket,
	}, func(objects *s3.ListObjectsOutput, lastPage bool) bool {
		for _, o := range objects.Contents {
			res[*o.Key] = *o
		}
		return true
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot list s3 objects")
	}

	return res, nil
}
