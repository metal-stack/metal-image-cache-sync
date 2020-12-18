package metrics

import (
	"os"
	"path/filepath"

	"github.com/metal-stack/metal-image-cache-sync/pkg/api"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type Collector struct {
	logger                    *zap.SugaredLogger
	config                    *api.Config
	cacheMissInc              func()
	cacheSyncDownloadBytesAdd func(float64)
	cacheSyncDownloadInc      func()
	cacheImageDownloadsInc    func()
}

func MustMetrics(logger *zap.SugaredLogger, config *api.Config) *Collector {
	c := &Collector{
		logger: logger,
		config: config,
	}

	cacheSize := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "current_cache_size",
		Help: "Current size of the cache directory in bytes",
	}, c.cacheDirSize)

	cacheMisses := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_misses",
		Help: "Amount of cache misses",
	})
	c.cacheMissInc = cacheMisses.Inc

	cacheSyncDownloadBytes := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_sync_downloaded_image_bytes",
		Help: "Amount of bytes downloaded by the image cache",
	})
	c.cacheSyncDownloadBytesAdd = cacheSyncDownloadBytes.Add

	cacheSyncDownloadCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_sync_downloaded_image_count",
		Help: "Amount of images downloaded by the image cache",
	})
	c.cacheSyncDownloadInc = cacheSyncDownloadCount.Inc

	cacheImageDownloads := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_image_downloads",
		Help: "Amount of images downloaded from the image cache",
	})
	c.cacheImageDownloadsInc = cacheImageDownloads.Inc

	prometheus.MustRegister(cacheSize)
	prometheus.MustRegister(cacheMisses)
	prometheus.MustRegister(cacheSyncDownloadBytes)
	prometheus.MustRegister(cacheSyncDownloadCount)
	prometheus.MustRegister(cacheImageDownloads)

	return c
}

func (c *Collector) cacheDirSize() float64 {
	var size int64
	err := filepath.Walk(c.config.ImageCacheRootPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})

	if err != nil {
		c.logger.Errorw("error collecting cache dir size metric", "error", err)
	}

	return float64(size)
}

func (c *Collector) IncrementCacheMiss() {
	c.cacheMissInc()
}

func (c *Collector) AddSyncDownloadImageBytes(b int64) {
	c.cacheSyncDownloadBytesAdd(float64(b))
}

func (c *Collector) IncrementSyncDownloadImageCount() {
	c.cacheSyncDownloadInc()
}

func (c *Collector) IncrementDownloadedImages() {
	c.cacheImageDownloadsInc()
}
