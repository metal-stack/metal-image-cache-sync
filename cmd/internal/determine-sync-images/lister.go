package synclister

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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
	logger    *zap.SugaredLogger
	driver    *metalgo.Driver
	config    *api.Config
	s3        *s3.S3
	collector *metrics.Collector
}

func NewSyncLister(logger *zap.SugaredLogger, driver *metalgo.Driver, s3 *s3.S3, collector *metrics.Collector, config *api.Config) *SyncLister {
	return &SyncLister{
		logger:    logger,
		driver:    driver,
		config:    config,
		s3:        s3,
		collector: collector,
	}
}

func (s *SyncLister) DetermineImageSyncList() ([]api.OS, error) {
	s3Images, err := s.retrieveImagesFromS3()
	if err != nil {
		return nil, errors.Wrap(err, "error listing images in s3")
	}

	resp, err := s.driver.ImageList()
	if err != nil {
		return nil, errors.Wrap(err, "error listing images")
	}

	s.collector.SetMetalAPIImageCount(len(resp.Image))

	expirationGraceDays := 24 * time.Hour * time.Duration(s.config.ExpirationGraceDays)

	images := api.OSImagesByOS{}
	for _, img := range resp.Image {
		if s.isExcluded(img.URL) {
			s.logger.Debugw("skipping image with exclude URL", "id", *img.ID)
			continue
		}

		if img.ExpirationDate != nil {
			if time.Since(time.Time(*img.ExpirationDate)) > expirationGraceDays {
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
			Name:       os,
			Version:    ver,
			ApiRef:     *img,
			BucketKey:  bucketKey,
			BucketName: s.config.ImageBucket,
			ImageRef:   s3Image,
			MD5Ref:     s3MD5,
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

func (s *SyncLister) isExcluded(url string) bool {
	for _, exclude := range s.config.ExcludePaths {
		if strings.Contains(url, exclude) {
			return true
		}
	}

	return false
}

func (s *SyncLister) DetermineKernelSyncList() ([]api.Kernel, error) {
	resp, err := s.driver.PartitionList()
	if err != nil {
		return nil, errors.Wrap(err, "error listing partitions")
	}

	var result []api.Kernel
	cachedKernelURLS := map[string]bool{}

	for _, p := range resp.Partition {
		if p.Bootconfig == nil {
			continue
		}

		kernelURL := p.Bootconfig.Kernelurl

		if cachedKernelURLS[kernelURL] {
			continue
		}

		if s.isExcluded(kernelURL) {
			s.logger.Debugw("skipping kernel with exclude URL", "url", kernelURL)
			continue
		}

		u, err := url.Parse(kernelURL)
		if err != nil {
			s.logger.Errorw("kernel url is invalid, skipping", "error", err)
			continue
		}

		size, err := retrieveContentLength(u.String())
		if err != nil {
			s.logger.Warnw("unable to determine kernel download size", "error", err)
		}

		result = append(result, api.Kernel{
			SubPath: strings.TrimPrefix(u.Path, "/"),
			URL:     kernelURL,
			Size:    size,
		})
	}

	return result, nil
}

func (s *SyncLister) DetermineBootImageSyncList() ([]api.BootImage, error) {
	resp, err := s.driver.PartitionList()
	if err != nil {
		return nil, errors.Wrap(err, "error listing partitions")
	}

	var result []api.BootImage
	cachedBootImageURLS := map[string]bool{}

	for _, p := range resp.Partition {
		if p.Bootconfig == nil {
			continue
		}

		bootImageURL := p.Bootconfig.Imageurl

		if cachedBootImageURLS[bootImageURL] {
			continue
		}

		if s.isExcluded(bootImageURL) {
			s.logger.Debugw("skipping boot image with exclude URL", "url", bootImageURL)
			continue
		}

		u, err := url.Parse(bootImageURL)
		if err != nil {
			s.logger.Errorw("boot image url is invalid, skipping", "error", err)
			continue
		}

		size, err := retrieveContentLength(u.String())
		if err != nil {
			s.logger.Warnw("unable to determine boot image download size", "error", err)
		}

		md5URL := u.String() + ".md5"
		_, err = retrieveContentLength(md5URL)
		if err != nil {
			s.logger.Errorw("boot image md5 does not exist, skipping", "url", md5URL, "error", err)
			continue
		}

		result = append(result, api.BootImage{
			SubPath: strings.TrimPrefix(u.Path, "/"),
			URL:     bootImageURL,
			Size:    size,
		})
	}

	return result, nil
}

func retrieveContentLength(url string) (int64, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, errors.Wrap(err, "unable issue HEAD request")
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HEAD request to url did not return OK: %s", url)
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return 0, errors.Wrap(err, "content-length header value could not be converted to integer")
	}

	return int64(size), nil
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
