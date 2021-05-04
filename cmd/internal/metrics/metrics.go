package metrics

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type DownloadCollector interface {
	IncrementCacheMiss()
	IncrementDownloads()

	GetGatherer() prometheus.Gatherer
}

func fileCount(path string) (int64, error) {
	var count int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasSuffix(info.Name(), ".md5") {
			count += 1
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return size, nil
}
