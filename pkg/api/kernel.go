package api

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
)

type Kernel struct {
	SubPath string
	URL     string
	Size    int64
}

func (k Kernel) GetName() string {
	return semverOrURL(k.URL)
}

func (k Kernel) GetSubPath() string {
	return k.SubPath
}

func (k Kernel) GetSize() int64 {
	return k.Size
}

func (k Kernel) HasMD5() bool {
	return false
}

func (k Kernel) DownloadMD5(ctx context.Context, target *afero.File, c *http.Client, s3downloader *s3manager.Downloader) (string, error) {
	return "", nil
}

func (k Kernel) Download(ctx context.Context, target afero.File, c *http.Client, s3downloader *s3manager.Downloader) (int64, error) {
	req, err := http.NewRequest(http.MethodGet, k.URL, nil)
	if err != nil {
		return 0, fmt.Errorf("unable to create get request:%w", err)
	}

	req = req.WithContext(ctx)

	resp, err := c.Do(req)
	if err != nil {
		return 0, fmt.Errorf("kernel download error:%w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	n, err := io.Copy(target, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("kernel download error:%w", err)
	}

	return n, nil
}
