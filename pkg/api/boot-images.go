package api

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type BootImage struct {
	Key  string
	URL  string
	Size int64
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

func (b BootImage) GetPath() string {
	return b.Key
}

func (b BootImage) GetSize() int64 {
	return b.Size
}

func (b BootImage) HasMD5() bool {
	return true
}

func (b BootImage) DownloadMD5(ctx context.Context, target *afero.File, s3downloader *s3manager.Downloader) (string, error) {
	md5URL := b.URL + ".md5"

	resp, err := http.Get(md5URL)
	if err != nil {
		return "", errors.Wrap(err, "boot image md5 download error")
	}
	defer resp.Body.Close()

	if target != nil {
		_, err = io.Copy(*target, resp.Body)
		if err != nil {
			return "", errors.Wrap(err, "boot image md5 download error")
		}

		return "", nil
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "boot image md5 download error")
	}

	parts := strings.Split(string(body), " ")
	if len(parts) == 0 {
		return "", fmt.Errorf("md5 sum file has unexpected format")
	}

	return parts[0], nil
}

func (b BootImage) Download(ctx context.Context, target afero.File, s3downloader *s3manager.Downloader) (int64, error) {
	resp, err := http.Get(b.URL)
	if err != nil {
		return 0, errors.Wrap(err, "boot image download error")
	}
	defer resp.Body.Close()

	n, err := io.Copy(target, resp.Body)
	if err != nil {
		return 0, errors.Wrap(err, "boot image download error")
	}

	return n, nil
}
