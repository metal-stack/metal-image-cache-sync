package main

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	metalgo "github.com/metal-stack/metal-go"
	synclister "github.com/metal-stack/metal-image-cache-sync/cmd/internal/determine-sync-images"
	"github.com/metal-stack/metal-image-cache-sync/cmd/internal/metrics"
	"github.com/metal-stack/metal-image-cache-sync/cmd/internal/sync"
	"github.com/metal-stack/metal-image-cache-sync/pkg/api"
	"github.com/metal-stack/metal-image-cache-sync/pkg/utils"
	"github.com/metal-stack/v"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron/v3"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

const (
	moduleName  = "metal-image-cache-sync"
	cfgFileType = "yaml"
)

var (
	cfgFile string
	lister  *synclister.SyncLister
	syncer  *sync.Syncer
	logger  *zap.SugaredLogger
	stop    context.Context
)

var rootCmd = &cobra.Command{
	Use:           moduleName,
	Short:         "a service that sync latest metal-stack images to a partition local image cache",
	Version:       v.V.String(),
	SilenceErrors: true,
	SilenceUsage:  true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		initConfig()
		initLogging()
		initSignalHandlers()
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return run()
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		if logger != nil {
			logger.Fatalw("failed executing root command", "error", err)
		} else {
			log.Fatal(err)
		}
	}
}

func init() {
	rootCmd.Flags().String("log-level", "info", "sets the application log level")

	rootCmd.Flags().String("image-store", "metal-stack.io", "url to the image store")
	rootCmd.Flags().String("image-store-bucket", "images", "bucket of the image store")

	rootCmd.Flags().String("metal-api-endpoint", "", "endpoint of the metal-api")
	rootCmd.Flags().String("metal-api-hmac", "", "hmac of the metal-api (requires view access)")

	rootCmd.Flags().String("schedule", "*/10 * * * *", "cron sync schedule")
	rootCmd.Flags().Bool("dry-run", false, "does not download any images, useful for development purposes")

	rootCmd.Flags().String("max-cache-size", "10G", "maximum size that the cache should have in the end (can exceed if min amount of images for all image variants is reached)")
	rootCmd.Flags().Int("min-images-per-name", 3, "minimum amount of images to keep of an image variant")
	rootCmd.Flags().Int("max-images-per-name", -1, "maximum amount of images to cache for an image variant, unlimited if less than zero")

	rootCmd.Flags().Uint("expiration-grace-period", 0, "the amount of days to still sync images even if they have already expired in the metal-api (defaults to zero)")

	rootCmd.Flags().String("cache-root-path", "/var/lib/metal-image-cache-sync", "root path of where to store the cached entities")

	rootCmd.Flags().String("image-cache-bind-address", "0.0.0.0:3000", "image cache http server bind address")

	rootCmd.Flags().Bool("enable-kernel-cache", true, "enables caching kernels used for PXE booting inside partitions")
	rootCmd.Flags().String("kernel-cache-bind-address", "0.0.0.0:3001", "kernel cache http server bind address")

	rootCmd.Flags().Bool("enable-boot-image-cache", true, "enables caching initrd images used for PXE booting inside partitions")
	rootCmd.Flags().String("boot-image-cache-bind-address", "0.0.0.0:3002", "kernel cache http server bind address")

	rootCmd.Flags().StringSlice("excludes", []string{"/pull_requests/"}, "url paths to exclude from the sync")

	err := viper.BindPFlags(rootCmd.Flags())
	if err != nil {
		log.Fatalf("error setup root cmd: %v", err)
	}
}

func initLogging() {
	level := zap.InfoLevel

	if viper.IsSet("log-level") {
		err := level.UnmarshalText([]byte(viper.GetString("log-level")))
		if err != nil {
			log.Fatalf("can't initialize zap logger: %v", err)
		}
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)

	l, err := cfg.Build()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}

	logger = l.Sugar()
}

func initConfig() {
	viper.SetEnvPrefix("METAL_IMAGE_CACHE_SYNC")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	viper.SetConfigType(cfgFileType)

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("config file path set explicitly, but unreadable: %v", err)
		}
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("/etc/" + moduleName)
		viper.AddConfigPath("$HOME/." + moduleName)
		viper.AddConfigPath(".")
		if err := viper.ReadInConfig(); err != nil {
			usedCfg := viper.ConfigFileUsed()
			if usedCfg != "" {
				log.Fatalf("config file %s unreadable: %v", usedCfg, err)
			}
		}
	}
}

func initSignalHandlers() {
	stop = signals.SetupSignalHandler()
}

func run() error {
	fs := afero.NewOsFs()

	c, err := api.NewConfig()
	if err != nil {
		logger.Errorw("error reading config", "error", err)
		return err
	}

	err = c.Validate(fs)
	if err != nil {
		logger.Errorw("error validating config", "error", err)
		return err
	}

	driver, err := metalgo.NewDriver(c.MetalAPIEndpoint, "", c.MetalAPIHMAC, metalgo.AuthType("Metal-View"))
	if err != nil {
		logger.Errorw("cannot create metal-api client", "error", err)
		return err
	}

	collector := metrics.MustMetrics(logger.Named("metrics"), c.CacheRootPath)

	dummyRegion := "dummy" // we don't use AWS S3, we don't need a proper region
	ss, err := session.NewSession(&aws.Config{
		Endpoint:    &c.ImageStore,
		Region:      &dummyRegion,
		Credentials: credentials.AnonymousCredentials,
		Retryer: client.DefaultRetryer{
			NumMaxRetries: 3,
			MinRetryDelay: 10 * time.Second,
		},
	})
	if err != nil {
		logger.Errorw("cannot create s3 client", "error", err)
		return err
	}

	s3Client := s3.New(ss)
	s3Downloader := s3manager.NewDownloader(ss)

	lister = synclister.NewSyncLister(logger.Named("sync-lister"), driver, s3Client, collector, c)

	syncer, err = sync.NewSyncer(logger.Named("syncer"), fs, s3Downloader, c, collector, stop)
	if err != nil {
		logger.Errorw("cannot create syncer", "error", err)
		return err
	}

	cronjob := cron.New(cron.WithChain(
		cron.SkipIfStillRunning(utils.NewCronLogger(logger.Named("cron"))),
	))

	id, err := cronjob.AddFunc(c.SyncSchedule, func() {
		err := runSync(c)
		if err != nil {
			logger.Errorw("error during sync", "error", err)
		}

		for _, e := range cronjob.Entries() {
			logger.Infow("scheduling next sync", "at", e.Next.String())
		}
	})
	if err != nil {
		return errors.Wrap(err, "could not initialize cron schedule")
	}

	handlers := []cacheFileHandler{newCacheFileHandler(c.ImageCacheBindAddress, c.GetImageRootPath(), collector)}
	if c.KernelCacheEnabled {
		// TODO: own collector for kernels
		handlers = append(handlers, newCacheFileHandler(c.KernelCacheBindAddress, c.GetKernelRootPath(), collector))
	}
	if c.BootImageCacheEnabled {
		// TODO: own collector for boot images
		handlers = append(handlers, newCacheFileHandler(c.BootImageCacheBindAddress, c.GetBootImageRootPath(), collector))
	}

	logger.Infow("start metal stack image sync", "version", v.V.String())

	var srvs []*http.Server
	for _, h := range handlers {
		h := h
		router := http.NewServeMux()

		router.Handle("/metrics", promhttp.Handler())
		router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("HEALTHY"))
			if err != nil {
				logger.Errorw("health endpoint could not write response body", "error", err)
			}
		})
		router.HandleFunc("/", h.handle)

		srv := http.Server{
			Addr:    h.bindAddress,
			Handler: router,
		}

		srvs = append(srvs, &srv)

		go func() {
			logger.Infow("starting to serve files", "bind-address", h.bindAddress, "directory", h.serveDir)
			err := srv.ListenAndServe()
			if err != nil {
				if err != http.ErrServerClosed {
					logger.Fatalw("error starting http server, shutting down...", "error", err)
				}
			}
		}()

	}

	err = runSync(c)
	if err != nil {
		logger.Errorw("error during initial sync", "error", err)
	}
	cronjob.Start()
	logger.Infow("scheduling next sync", "at", cronjob.Entry(id).Next.String())

	<-stop.Done()
	logger.Info("received stop signal, shutting down...")
	cronjob.Stop()

	for _, srv := range srvs {
		err = srv.Close()
		if err != nil {
			logger.Errorw("error shutting down http server", "error", err)
		}
	}

	return nil

}

type cacheFileHandler struct {
	serveDir     string
	serveHandler http.Handler
	collector    *metrics.Collector
	bindAddress  string
}

func newCacheFileHandler(bindAddr, serveDir string, collector *metrics.Collector) cacheFileHandler {
	return cacheFileHandler{
		serveDir:     serveDir,
		serveHandler: http.FileServer(http.Dir(serveDir)),
		collector:    collector,
		bindAddress:  bindAddr,
	}
}

func (c *cacheFileHandler) handle(w http.ResponseWriter, r *http.Request) {
	logger.Infow("serving cache download request", "url", r.URL.String(), "from", r.RemoteAddr)
	hw := utils.NewHTTPStatusResponseWriter(w)
	c.serveHandler.ServeHTTP(hw, r)
	switch code := hw.GetStatus(); code {
	case http.StatusNotFound:
		logger.Infow("cache miss", "url", r.URL.String())
		c.collector.IncrementCacheMiss()
	case http.StatusOK:
		c.collector.IncrementDownloadedImages()
	case 0:
		// occurs when just visting directories through browser, swallow
	default:
		logger.Infow("responded with error code for download", "url", r.URL.String(), "code", code)
	}
}

func runSync(c *api.Config) error {
	var errs []error

	err := func() error {
		syncImages, err := lister.DetermineImageSyncList()
		if err != nil {
			return errors.Wrap(err, "cannot gather images")
		}

		var converted api.CacheEntities
		for _, s := range syncImages {
			converted = append(converted, s)
		}

		err = syncer.Sync(c.GetImageRootPath(), converted)
		if err != nil {
			return errors.Wrap(err, "error during image sync")
		}

		return nil
	}()
	if err != nil {
		errs = append(errs, err)
	}

	err = func() error {
		syncKernels, err := lister.DetermineKernelSyncList()
		if err != nil {
			return errors.Wrap(err, "cannot kernel images")
		}

		var converted api.CacheEntities
		for _, s := range syncKernels {
			converted = append(converted, s)
		}

		err = syncer.Sync(c.GetKernelRootPath(), converted)
		if err != nil {
			return errors.Wrap(err, "error during kernel sync")
		}

		return nil
	}()
	if err != nil {
		errs = append(errs, err)
	}

	err = func() error {
		syncImages, err := lister.DetermineBootImageSyncList()
		if err != nil {
			return errors.Wrap(err, "cannot gather boot images")
		}

		var converted api.CacheEntities
		for _, s := range syncImages {
			converted = append(converted, s)
		}

		err = syncer.Sync(c.GetBootImageRootPath(), converted)
		if err != nil {
			return errors.Wrap(err, "error during boot image sync")
		}

		return nil
	}()
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Errorf("errors occurred during sync: %v", errs)
	}

	return nil
}
