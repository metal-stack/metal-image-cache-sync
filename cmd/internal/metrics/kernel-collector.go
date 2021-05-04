package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type KernelCollector struct {
	logger         *zap.SugaredLogger
	reg            *prometheus.Registry
	rootPath       string
	cacheMissInc   func()
	cacheDownloads func()
}

func MustKernelMetrics(logger *zap.SugaredLogger, rootPath string) *KernelCollector {
	c := &KernelCollector{
		logger:   logger,
		rootPath: rootPath,
		reg:      prometheus.NewRegistry(),
	}

	cacheSize := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "current_cache_size",
		Help: "Current size of the cache directory in bytes",
	}, c.cacheDirSize)

	cacheImageCount := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cache_kernel_count",
		Help: "Current amount of kernels in the cache (amount of files in cache directory excluding checksums)",
	}, c.cacheImageCount)

	cacheMisses := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_misses",
		Help: "Amount of cache misses during instance lifetime",
	})
	c.cacheMissInc = cacheMisses.Inc

	cacheDownloads := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_downloads",
		Help: "Amount of kernels downloaded from the kernel cache during instance lifetime",
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

func (c *KernelCollector) cacheDirSize() float64 {
	fmt.Println("Counting cache dir size")
	size, err := dirSize(c.rootPath)

	if err != nil {
		c.logger.Errorw("error collecting cache dir size metric", "error", err)
	}

	return float64(size)
}

func (c *KernelCollector) cacheImageCount() float64 {
	count, err := fileCount(c.rootPath)

	if err != nil {
		c.logger.Errorw("error collecting image cache count metric", "error", err)
	}

	return float64(count)
}

func (c *KernelCollector) IncrementCacheMiss() {
	c.cacheMissInc()
}

func (c *KernelCollector) IncrementDownloads() {
	c.cacheDownloads()
}

func (c *KernelCollector) GetGatherer() prometheus.Gatherer {
	return c.reg
}
