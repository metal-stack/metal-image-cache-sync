package api

import (
	"context"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
)

type CacheEntities []CacheEntity

type CacheEntity interface {
	GetID() string
	GetPath() string
	GetSize() int64
	HasMD5() bool
	DownloadMD5(ctx context.Context, target *afero.File, s3downloader *s3manager.Downloader) (string, error)
	Download(ctx context.Context, target afero.File, s3downloader *s3manager.Downloader) (int64, error)
}
