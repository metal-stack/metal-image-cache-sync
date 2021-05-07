package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type ImageCollector struct {
	logger                    *zap.SugaredLogger
	reg                       *prometheus.Registry
	rootPath                  string
	cacheMissInc              func()
	cacheSyncDownloadBytesAdd func(float64)
	cacheSyncDownloadInc      func()
	cacheDownloadsInc         func()
	cacheUnsyncedImageCount   func(float64)
	metalAPIImageCount        func(float64)
}

func MustImageMetrics(logger *zap.SugaredLogger, rootPath string) *ImageCollector {
	c := &ImageCollector{
		logger:   logger,
		rootPath: rootPath,
		reg:      prometheus.NewRegistry(),
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

	cacheDownloadsInc := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_image_downloads",
		Help: "Amount of images downloaded from the image cache during instance lifetime",
	})
	c.cacheDownloadsInc = cacheDownloadsInc.Inc

	c.reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	c.reg.MustRegister(prometheus.NewGoCollector())
	c.reg.MustRegister(cacheSize)
	c.reg.MustRegister(cacheImageCount)
	c.reg.MustRegister(cacheUnsyncedImageCount)
	c.reg.MustRegister(cacheMisses)
	c.reg.MustRegister(cacheSyncDownloadBytes)
	c.reg.MustRegister(cacheSyncDownloadCount)
	c.reg.MustRegister(cacheDownloadsInc)
	c.reg.MustRegister(metalImageCount)

	return c
}

func (c *ImageCollector) cacheDirSize() float64 {
	size, err := dirSize(c.rootPath)

	if err != nil {
		c.logger.Errorw("error collecting cache dir size metric", "error", err)
	}

	return float64(size)
}

func (c *ImageCollector) cacheImageCount() float64 {
	count, err := fileCount(c.rootPath)

	if err != nil {
		c.logger.Errorw("error collecting image cache count metric", "error", err)
	}

	return float64(count)
}

func (c *ImageCollector) IncrementCacheMiss() {
	c.cacheMissInc()
}

func (c *ImageCollector) AddSyncDownloadImageBytes(b int64) {
	c.cacheSyncDownloadBytesAdd(float64(b))
}

func (c *ImageCollector) IncrementSyncDownloadImageCount() {
	c.cacheSyncDownloadInc()
}

func (c *ImageCollector) SetUnsyncedImageCount(b int) {
	c.cacheUnsyncedImageCount(float64(b))
}

func (c *ImageCollector) IncrementDownloads() {
	c.cacheDownloadsInc()
}

func (c *ImageCollector) SetMetalAPIImageCount(b int) {
	c.metalAPIImageCount(float64(b))
}

func (c *ImageCollector) GetGatherer() prometheus.Gatherer {
	return c.reg
}
