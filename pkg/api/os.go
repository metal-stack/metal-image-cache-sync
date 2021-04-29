package api

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/metal-stack/metal-go/api/models"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type OS struct {
	Name       string
	Version    *semver.Version
	ApiRef     models.V1ImageResponse
	ImageRef   s3.Object
	MD5Ref     s3.Object
	BucketKey  string
	BucketName string
}
type OSImagesByVersion map[string][]OS
type OSImagesByOS map[string]OSImagesByVersion

func SortOSImagesByName(imgs []OS) {
	sort.Slice(imgs, func(i, j int) bool {
		if imgs[i].Name == imgs[j].Name {
			return imgs[i].Version.LessThan(imgs[j].Version)
		}
		return strings.Compare(imgs[i].Name, imgs[j].Name) < 0
	})
}

func (o *OS) MajorMinor() (string, error) {
	if o.Version == nil {
		return "", fmt.Errorf("image version is nil")
	}
	return fmt.Sprintf("%d.%d", o.Version.Major(), o.Version.Minor()), nil
}

func (o OS) GetName() string {
	if o.ApiRef.ID == nil {
		return ""
	}
	return *o.ApiRef.ID
}

func (o OS) GetPath() string {
	return o.BucketKey
}

func (o OS) GetSize() int64 {
	if o.ImageRef.Size == nil {
		return 0
	}
	return *o.ImageRef.Size
}

func (o OS) HasMD5() bool {
	return true
}

func (o OS) DownloadMD5(ctx context.Context, target *afero.File, s3downloader *s3manager.Downloader) (string, error) {
	if target != nil {
		_, err := s3downloader.DownloadWithContext(ctx, *target, &s3.GetObjectInput{
			Bucket: &o.BucketName,
			Key:    o.MD5Ref.Key,
		})
		if err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("error downloading checksum of image: %s", o.BucketKey))
		}
		return "", nil
	}

	buff := &aws.WriteAtBuffer{}
	_, err := s3downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: &o.BucketName,
		Key:    o.MD5Ref.Key,
	})
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("error downloading checksum of image: %s", o.BucketKey))
	}

	parts := strings.Split(string(buff.Bytes()), " ")
	if len(parts) == 0 {
		return "", fmt.Errorf("md5 sum file has unexpected format")
	}

	return parts[0], nil
}

func (o OS) Download(ctx context.Context, target afero.File, s3downloader *s3manager.Downloader) (int64, error) {
	n, err := s3downloader.DownloadWithContext(ctx, target, &s3.GetObjectInput{
		Bucket: &o.BucketName,
		Key:    &o.BucketKey,
	})
	if err != nil {
		return 0, errors.Wrap(err, "image download error")
	}

	return n, nil
}
