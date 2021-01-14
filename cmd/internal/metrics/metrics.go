package metrics

import (
	"os"
	"path/filepath"
	"strings"

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
	cacheUnsyncedImageCount   func(float64)
	metalAPIImageCount        func(float64)
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

	cacheImageCount := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cache_image_count",
		Help: "Current amount of images in the cache (amount of files in cache directory excluding checksums)",
	}, c.cacheImageCount)

	cacheUnsyncedImageCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_unsynced_image_count",
		Help: "Amount of images from the metal-api not synced into the cache (due to expiration, cache size constraints, ...)",
	})
	c.cacheUnsyncedImageCount = cacheUnsyncedImageCount.Set

	metalImageCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "metal_api_image_count",
		Help: "Amount of images configured in the metal-api",
	})
	c.metalAPIImageCount = metalImageCount.Set

	cacheMisses := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_misses",
		Help: "Amount of cache misses during instance lifetime",
	})
	c.cacheMissInc = cacheMisses.Inc

	cacheSyncDownloadBytes := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_sync_downloaded_image_bytes",
		Help: "Amount of bytes downloaded by the image cache during instance lifetime",
	})
	c.cacheSyncDownloadBytesAdd = cacheSyncDownloadBytes.Add

	cacheSyncDownloadCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_sync_downloaded_image_count",
		Help: "Amount of images downloaded by the image cache during instance lifetime",
	})
	c.cacheSyncDownloadInc = cacheSyncDownloadCount.Inc

	cacheImageDownloads := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_image_downloads",
		Help: "Amount of images downloaded from the image cache during instance lifetime",
	})
	c.cacheImageDownloadsInc = cacheImageDownloads.Inc

	prometheus.MustRegister(cacheSize)
	prometheus.MustRegister(cacheImageCount)
	prometheus.MustRegister(cacheUnsyncedImageCount)
	prometheus.MustRegister(cacheMisses)
	prometheus.MustRegister(cacheSyncDownloadBytes)
	prometheus.MustRegister(cacheSyncDownloadCount)
	prometheus.MustRegister(cacheImageDownloads)

	prometheus.MustRegister(metalImageCount)

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
		return nil
	})

	if err != nil {
		c.logger.Errorw("error collecting cache dir size metric", "error", err)
	}

	return float64(size)
}

func (c *Collector) cacheImageCount() float64 {
	var count int64
	err := filepath.Walk(c.config.ImageCacheRootPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasSuffix(info.Name(), ".md5") {
			count += 1
		}
		return nil
	})

	if err != nil {
		c.logger.Errorw("error collecting image cache count metric", "error", err)
	}

	return float64(count)
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

func (c *Collector) SetUnsyncedImageCount(b int) {
	c.cacheUnsyncedImageCount(float64(b))
}

func (c *Collector) IncrementDownloadedImages() {
	c.cacheImageDownloadsInc()
}

func (c *Collector) SetMetalAPIImageCount(b int) {
	c.metalAPIImageCount(float64(b))
}
