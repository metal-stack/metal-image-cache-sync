package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/afero"
)

type BootImage struct {
	SubPath string
	URL     string
	Size    int64
}

func (b BootImage) GetName() string {
	// try to find a semver version somewhere in the path...
	for _, p := range strings.Split(b.URL, "/") {
		version, err := semver.NewVersion(strings.TrimPrefix(p, "v"))
		if err == nil {
			return version.String()
		}
	}
	return b.URL
}

func (b BootImage) GetSubPath() string {
	return b.SubPath
}

func (b BootImage) GetSize() int64 {
	return b.Size
}

func (b BootImage) HasMD5() bool {
	return true
}

func (b BootImage) DownloadMD5(ctx context.Context, target *afero.File, c *http.Client, s3downloader *s3manager.Downloader) (string, error) {
	md5URL := b.URL + ".md5"

	req, err := http.NewRequest(http.MethodGet, md5URL, nil)
	if err != nil {
		return "", fmt.Errorf("unable to create get request:%w", err)
	}

	req = req.WithContext(ctx)

	resp, err := c.Do(req)
	if err != nil {
		return "", fmt.Errorf("boot image md5 download error:%w", err)
	}
	defer resp.Body.Close()

	if target != nil {
		_, err = io.Copy(*target, resp.Body)
		if err != nil {
			return "", fmt.Errorf("boot image md5 download error:%w", err)
		}

		return "", nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("boot image md5 download error:%w", err)
	}

	parts := strings.Split(string(body), " ")
	if len(parts) == 0 {
		return "", fmt.Errorf("md5 sum file has unexpected format:%w", err)
	}

	return parts[0], nil
}

func (b BootImage) Download(ctx context.Context, target afero.File, c *http.Client, s3downloader *s3manager.Downloader) (int64, error) {
	req, err := http.NewRequest(http.MethodGet, b.URL, nil)
	if err != nil {
		return 0, fmt.Errorf("unable to create get request:%w", err)
	}

	req = req.WithContext(ctx)

	resp, err := c.Do(req)
	if err != nil {
		return 0, fmt.Errorf("boot image download error:%w", err)
	}
	defer resp.Body.Close()

	n, err := io.Copy(target, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("boot image download error:%w", err)
	}

	return n, nil
}
