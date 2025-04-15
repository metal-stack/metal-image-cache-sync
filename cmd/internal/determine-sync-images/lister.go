package synclister

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	metalgo "github.com/metal-stack/metal-go"
	"github.com/metal-stack/metal-go/api/client/image"
	"github.com/metal-stack/metal-go/api/client/partition"
	"github.com/metal-stack/metal-image-cache-sync/cmd/internal/metrics"
	"github.com/metal-stack/metal-image-cache-sync/pkg/api"
	"github.com/metal-stack/metal-image-cache-sync/pkg/utils"
)

type SyncLister struct {
	logger         *slog.Logger
	client         metalgo.Client
	config         *api.Config
	s3             *s3.S3
	stop           context.Context
	imageCollector *metrics.ImageCollector
	httpClient     *http.Client
}

func NewSyncLister(logger *slog.Logger, client metalgo.Client, s3 *s3.S3, imageCollector *metrics.ImageCollector, config *api.Config, stop context.Context) *SyncLister {
	return &SyncLister{
		logger:         logger,
		client:         client,
		config:         config,
		s3:             s3,
		stop:           stop,
		imageCollector: imageCollector,
		httpClient:     http.DefaultClient,
	}
}

func (s *SyncLister) DetermineImageSyncList() ([]api.OS, error) {
	s3Images, err := s.retrieveImagesFromS3()
	if err != nil {
		return nil, fmt.Errorf("error listing images in s3:%w", err)
	}

	resp, err := s.client.Image().ListImages(image.NewListImagesParams(), nil)
	if err != nil {
		return nil, fmt.Errorf("error listing images:%w", err)
	}

	s.imageCollector.SetMetalAPIImageCount(len(resp.Payload))

	expirationGraceDays := 24 * time.Hour * time.Duration(s.config.ExpirationGraceDays) // nolint:gosec

	images := api.OSImagesByOS{}
	for _, img := range resp.Payload {
		if s.isExcluded(img.URL) {
			s.logger.Debug("skipping image with exclude URL", "id", *img.ID)
			continue
		}

		if img.ExpirationDate != nil {
			if time.Since(time.Time(*img.ExpirationDate)) > expirationGraceDays {
				s.logger.Debug("not considering expired image, skipping", "id", *img.ID)
				continue
			}
		}

		os, ver, err := utils.GetOsAndSemver(*img.ID)
		if err != nil {
			s.logger.Error("could not extract os and version, skipping", "image", img.ID, "error", err)
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
			s.logger.Error("image url is invalid, skipping", "error", err)
			continue
		}

		bucketKey := u.Path[1:]

		s3Image, ok := s3Images[bucketKey]
		if !ok {
			s.logger.Error("image is not contained in global image store, skipping", "path", u.Path, "id", *img.ID)
			continue
		}

		s3MD5, ok := s3Images[bucketKey+".md5"]
		if !ok {
			s.logger.Error("image md5 is not contained in global image store, skipping", "path", u.Path, "id", *img.ID)
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
		for _, versionedImages := range versions {
			versionedImages := versionedImages
			sort.Slice(versionedImages, func(i, j int) bool {
				return versionedImages[i].Version.GreaterThan(versionedImages[j].Version)
			})
			amount := 0
			for _, img := range versionedImages {
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

	for sizeCount < s.config.MaxCacheSize {
		syncImages, sizeCount, err = s.reduce(syncImages, sizeCount)
		if err != nil {
			s.logger.Warn("cannot reduce anymore images (all at minimum size), exceeding maximum cache size")
			break
		}
	}

	s.imageCollector.SetUnsyncedImageCount(len(resp.Payload) - len(syncImages))

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
	resp, err := s.client.Partition().ListPartitions(partition.NewListPartitionsParams(), nil)
	if err != nil {
		return nil, fmt.Errorf("error listing partitions:%w", err)
	}

	var result []api.Kernel
	urls := map[string]bool{}

	for _, p := range resp.Payload {
		if p.Bootconfig == nil {
			continue
		}

		kernelURL := p.Bootconfig.Kernelurl

		if urls[kernelURL] {
			continue
		}

		if s.isExcluded(kernelURL) {
			s.logger.Debug("skipping kernel with exclude URL", "url", kernelURL)
			continue
		}

		u, err := url.Parse(kernelURL)
		if err != nil {
			s.logger.Error("kernel url is invalid, skipping", "error", err)
			continue
		}

		size, err := retrieveContentLength(s.stop, s.httpClient, u.String())
		if err != nil {
			s.logger.Warn("unable to determine kernel download size", "error", err)
		}

		result = append(result, api.Kernel{
			SubPath: strings.TrimPrefix(u.Path, "/"),
			URL:     kernelURL,
			Size:    size,
		})
		urls[kernelURL] = true
	}

	return result, nil
}

func (s *SyncLister) DetermineBootImageSyncList() ([]api.BootImage, error) {
	resp, err := s.client.Partition().ListPartitions(partition.NewListPartitionsParams(), nil)
	if err != nil {
		return nil, fmt.Errorf("error listing partitions:%w", err)
	}

	var result []api.BootImage
	urls := map[string]bool{}

	for _, p := range resp.Payload {
		if p.Bootconfig == nil {
			continue
		}

		bootImageURL := p.Bootconfig.Imageurl

		if urls[bootImageURL] {
			continue
		}

		if s.isExcluded(bootImageURL) {
			s.logger.Debug("skipping boot image with exclude URL", "url", bootImageURL)
			continue
		}

		u, err := url.Parse(bootImageURL)
		if err != nil {
			s.logger.Error("boot image url is invalid, skipping", "error", err)
			continue
		}

		size, err := retrieveContentLength(s.stop, s.httpClient, u.String())
		if err != nil {
			s.logger.Warn("unable to determine boot image download size", "error", err)
		}

		md5URL := u.String() + ".md5"
		_, err = retrieveContentLength(s.stop, s.httpClient, md5URL)
		if err != nil {
			s.logger.Error("boot image md5 does not exist, skipping", "url", md5URL, "error", err)
			continue
		}

		result = append(result, api.BootImage{
			SubPath: strings.TrimPrefix(u.Path, "/"),
			URL:     bootImageURL,
			Size:    size,
		})
		urls[bootImageURL] = true
	}

	return result, nil
}

func retrieveContentLength(ctx context.Context, c *http.Client, url string) (int64, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return 0, fmt.Errorf("unable to create head request:%w", err)
	}

	req = req.WithContext(ctx)

	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("head request to url did not return OK: %s", url)
	}

	size, err := strconv.Atoi(resp.Header.Get("Content-Length"))
	if err != nil {
		return 0, fmt.Errorf("content-length header value could not be converted to integer:%w", err)
	}

	return int64(size), nil // nolint:gosec
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
		return nil, fmt.Errorf("cannot list s3 objects:%w", err)
	}

	return res, nil
}
