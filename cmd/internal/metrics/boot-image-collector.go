package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type BootImageCollector struct {
	logger         *zap.SugaredLogger
	reg            *prometheus.Registry
	rootPath       string
	cacheMissInc   func()
	cacheDownloads func()
}

func MustBootImageMetrics(logger *zap.SugaredLogger, rootPath string) *BootImageCollector {
	c := &BootImageCollector{
		logger:   logger,
		rootPath: rootPath,
		reg:      prometheus.NewRegistry(),
	}

	cacheSize := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "current_cache_size",
		Help: "Current size of the cache directory in bytes",
	}, c.cacheDirSize)

	cacheImageCount := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cache_boot_images_count",
		Help: "Current amount of bootimages in the cache (amount of files in cache directory excluding checksums)",
	}, c.cacheImageCount)

	cacheMisses := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_misses",
		Help: "Amount of cache misses during instance lifetime",
	})
	c.cacheMissInc = cacheMisses.Inc

	cacheDownloads := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_downloads",
		Help: "Amount of boot images downloaded from the boot image cache during instance lifetime",
	})
	c.cacheDownloads = cacheDownloads.Inc

	c.reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	c.reg.MustRegister(prometheus.NewGoCollector())
	c.reg.MustRegister(cacheSize)
	c.reg.MustRegister(cacheImageCount)
	c.reg.MustRegister(cacheMisses)
	c.reg.MustRegister(cacheDownloads)

	return c
}

func (c *BootImageCollector) cacheDirSize() float64 {
	size, err := dirSize(c.rootPath)

	if err != nil {
		c.logger.Errorw("error collecting cache dir size metric", "error", err)
	}

	return float64(size)
}

func (c *BootImageCollector) cacheImageCount() float64 {
	count, err := fileCount(c.rootPath)

	if err != nil {
		c.logger.Errorw("error collecting image cache count metric", "error", err)
	}

	return float64(count)
}

func (c *BootImageCollector) IncrementCacheMiss() {
	c.cacheMissInc()
}

func (c *BootImageCollector) IncrementDownloads() {
	c.cacheDownloads()
}

func (c *BootImageCollector) GetGatherer() prometheus.Gatherer {
	return c.reg
}
