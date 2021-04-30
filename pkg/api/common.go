package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
)

type CacheEntities []CacheEntity

type CacheEntity interface {
	GetName() string
	GetSubPath() string
	GetSize() int64
	HasMD5() bool
	DownloadMD5(ctx context.Context, target *afero.File, c *http.Client, s3downloader *s3manager.Downloader) (string, error)
	Download(ctx context.Context, target afero.File, c *http.Client, s3downloader *s3manager.Downloader) (int64, error)
}

type LocalFile struct {
	Name    string
	SubPath string
	Size    int64
}

func (l LocalFile) GetName() string {
	return l.Name
}

func (l LocalFile) GetSubPath() string {
	return l.SubPath
}

func (l LocalFile) GetSize() int64 {
	return l.Size
}

func (l LocalFile) HasMD5() bool {
	return false
}

func (l LocalFile) DownloadMD5(ctx context.Context, target *afero.File, c *http.Client, s3downloader *s3manager.Downloader) (string, error) {
	return "", nil
}

func (l LocalFile) Download(ctx context.Context, target afero.File, c *http.Client, s3downloader *s3manager.Downloader) (int64, error) {
	return 0, fmt.Errorf("not implemented on local file")
}
