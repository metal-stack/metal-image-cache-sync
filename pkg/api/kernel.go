package api

import (
	"context"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Kernel struct {
	Key  string
	URL  string
	Size int64
}

func (k Kernel) GetID() string {
	return k.URL
}

func (k Kernel) GetPath() string {
	return k.Key
}

func (k Kernel) GetSize() int64 {
	return k.Size
}

func (k Kernel) HasMD5() bool {
	return false
}

func (k Kernel) DownloadMD5(ctx context.Context, target *afero.File, s3downloader *s3manager.Downloader) (string, error) {
	return "", nil
}

func (k Kernel) Download(ctx context.Context, target afero.File, s3downloader *s3manager.Downloader) (int64, error) {
	resp, err := http.Get(k.URL)
	if err != nil {
		return 0, errors.Wrap(err, "kernel download error")
	}
	defer resp.Body.Close()

	n, err := io.Copy(target, resp.Body)
	if err != nil {
		return 0, errors.Wrap(err, "kernel download error")
	}

	return n, nil
}
