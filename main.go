package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docker/go-units"
	metalgo "github.com/metal-stack/metal-go"
	"github.com/metal-stack/metal-go/api/models"
	"github.com/metal-stack/v"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

type OS struct {
	Name      string
	Version   *semver.Version
	ApiRef    models.V1ImageResponse
	ImageRef  s3.Object
	MD5Ref    s3.Object
	BucketKey string
}
type OSImagesByVersion map[string][]OS
type OSImagesByOS map[string]OSImagesByVersion

type Config struct {
	MinImagesPerName int
	MaxImagesPerName int
	MaxCacheSize     int64

	ImageStore  string
	ImageBucket string

	MetalAPIEndpoint string
	MetalAPIHMAC     string

	SyncSchedule string
}

type CronLogger struct {
	l *zap.SugaredLogger
}

func (c *CronLogger) Info(msg string, keysAndValues ...interface{}) {
	c.l.Infow(msg, keysAndValues)
}

func (c *CronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	c.l.Errorw(msg, keysAndValues)
}

const (
	moduleName  = "metal-image-cache-sync"
	cfgFileType = "yaml"

	logLevelFlg = "log-level"
)

var (
	cfgFile string
	c       *Config
	logger  *zap.SugaredLogger
	stop    <-chan struct{}
)

var rootCmd = &cobra.Command{
	Use:           moduleName,
	Short:         "a service that sync latest metal-stack images to a partition local image cache",
	Version:       v.V.String(),
	SilenceErrors: true,
	SilenceUsage:  true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		initLogging()
		initConfig()
		initSignalHandlers()
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return run()
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		logger.Fatalw("failed executing root command", "error", err)
	}
}

func init() {
	c = &Config{}

	rootCmd.Flags().StringVar(&c.ImageStore, "image-store", "metal-stack.io", "url to the image store")
	rootCmd.Flags().StringVar(&c.ImageBucket, "image-store-bucket", "images", "bucket of the image store")

	rootCmd.Flags().StringVar(&c.MetalAPIEndpoint, "metal-api-endpoint", "", "endpoint of the metal-api")
	rootCmd.Flags().StringVar(&c.MetalAPIHMAC, "metal-api-hmac", "", "hmac of the metal-api (requires view access)")

	err := rootCmd.MarkFlagRequired("metal-api-endpoint")
	if err != nil {
		log.Fatalf("error setup root cmd: %v", err)
	}
	err = rootCmd.MarkFlagRequired("metal-api-hmac")
	if err != nil {
		log.Fatalf("error setup root cmd: %v", err)
	}

	rootCmd.Flags().StringVar(&c.SyncSchedule, "schedule", "*/10 * * * *", "cron sync schedule")

	rootCmd.Flags().String("max-cache-size", "10G", "maximum size that the cache should have in the end (can exceed if min amount of images for all image variants is reached)")
	rootCmd.Flags().IntVar(&c.MinImagesPerName, "min-images-per-name", 3, "minimum amount of images to keep of an image variant")
	rootCmd.Flags().IntVar(&c.MaxImagesPerName, "max-images-per-name", 10, "maximum amount of images to cache for an image variant")

	err = viper.BindPFlags(rootCmd.Flags())
	if err != nil {
		log.Fatalf("error setup root cmd: %v", err)
	}
}

func initLogging() {
	level := zap.InfoLevel

	if viper.IsSet(logLevelFlg) {
		err := level.UnmarshalText([]byte(viper.GetString(logLevelFlg)))
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
			logger.Fatalw("config file path set explicitly, but unreadable", "error", err)
		}
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("/etc/" + moduleName)
		viper.AddConfigPath("$HOME/." + moduleName)
		viper.AddConfigPath(".")
		if err := viper.ReadInConfig(); err != nil {
			usedCfg := viper.ConfigFileUsed()
			if usedCfg != "" {
				logger.Fatalw("config file unreadable", "config-file", usedCfg, "error", err)
			}
		}
	}

	usedCfg := viper.ConfigFileUsed()
	if usedCfg != "" {
		logger.Infow("read config file", "config-file", usedCfg)
	}

	var err error
	c.MaxCacheSize, err = units.FromHumanSize(viper.GetString("max-cache-size"))
	if err != nil {
		logger.Fatalw("cannot read max cache size", "error", err)
	}
}

func initSignalHandlers() {
	stop = signals.SetupSignalHandler()
}

func run() error {
	driver, err := metalgo.NewDriver(c.MetalAPIEndpoint, "", c.MetalAPIHMAC, metalgo.AuthType("Metal-View"))
	if err != nil {
		logger.Fatalw("cannot create metal-api client", "error", err)
	}

	cronjob := cron.New(cron.WithChain(
		cron.SkipIfStillRunning(&CronLogger{l: logger.Named("cron")}),
	))

	id, err := cronjob.AddFunc(c.SyncSchedule, func() {
		err := sync(driver)
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

	logger.Infow("start metal stack image sync", "version", v.V.String())

	err = sync(driver)
	if err != nil {
		logger.Errorw("error during initial sync", "error", err)
	}
	cronjob.Start()
	logger.Infow("scheduling next sync", "at", cronjob.Entry(id).Next.String())

	<-stop
	logger.Info("received stop signal, shutting down...")

	cronjob.Stop()
	return nil
}

func sync(driver *metalgo.Driver) error {
	syncImages, size, err := gatherImages(driver)
	if err != nil {
		logger.Fatalw("cannot gather images", "error", err)
	}

	logger.Infow("gathered images to sync", "amount", len(syncImages), "cache-size", units.BytesSize(float64(size)))
	data := [][]string{}
	for _, img := range syncImages {
		data = append(data, []string{*img.ApiRef.ID, img.BucketKey, units.HumanSize(float64(*img.ImageRef.Size))})
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Path", "Size"})

	for _, v := range data {
		table.Append(v)
	}
	table.Render()

	err = download(syncImages)
	if err != nil {
		return errors.Wrap(err, "error during image download")
	}

	err = cleanup(syncImages)
	if err != nil {
		return errors.Wrap(err, "error during image cleanup")
	}

	return nil
}

func gatherImages(driver *metalgo.Driver) ([]OS, int64, error) {
	s3Images, err := retrieveImagesFromS3()
	if err != nil {
		return nil, 0, errors.Wrap(err, "error listing images in s3")
	}

	apiImages, err := driver.ImageList()
	if err != nil {
		return nil, 0, errors.Wrap(err, "error listing images")
	}

	images := OSImagesByOS{}
	for _, img := range apiImages.Image {
		if img.ExpirationDate != nil {
			if time.Since(time.Time(*img.ExpirationDate)) > 0 {
				logger.Debugw("not considering expired image, skipping", "id", *img.ID)
				continue
			}
		}

		os, ver, err := GetOsAndSemver(*img.ID)
		if err != nil {
			logger.Errorw("could not extract os and version, skipping", "error", err)
			continue
		}

		versions, ok := images[os]
		if !ok {
			versions = OSImagesByVersion{}
		}

		majorMinor := fmt.Sprintf("%d.%d", ver.Major(), ver.Minor())
		imageVersions := versions[majorMinor]

		u, err := url.Parse(img.URL)
		if err != nil {
			logger.Errorw("image url is invalid, skipping", "error", err)
			continue
		}

		bucketKey := u.Path[1:]

		s3Image, ok := s3Images[bucketKey]
		if !ok {
			logger.Errorw("image is not contained in global image store, skipping", "path", u.Path, "id", *img.ID)
			continue
		}

		s3MD5, ok := s3Images[bucketKey+".md5"]
		if !ok {
			logger.Errorw("image md5 is not contained in global image store, skipping", "path", u.Path, "id", *img.ID)
			continue
		}

		imageVersions = append(imageVersions, OS{
			Name:      os,
			Version:   ver,
			ApiRef:    *img,
			BucketKey: bucketKey,
			ImageRef:  s3Image,
			MD5Ref:    s3MD5,
		})

		versions[majorMinor] = imageVersions
		images[os] = versions
	}

	var sizeCount int64
	var syncImages []OS
	for _, versions := range images {
		for _, images := range versions {
			sort.Slice(images, func(i, j int) bool {
				return images[i].Version.GreaterThan(images[j].Version)
			})
			amount := 0
			for _, img := range images {
				if amount >= c.MaxImagesPerName {
					break
				}
				amount += 1
				sizeCount += *img.ImageRef.Size
				syncImages = append(syncImages, img)
			}
		}
	}

	sortOSImagesByName(syncImages)

	for {
		if sizeCount < c.MaxCacheSize {
			break
		}

		syncImages, sizeCount, err = reduce(syncImages, sizeCount)
		if err != nil {
			logger.Warn("cannot reduce anymore images (all at minimum size), exceeding maximum cache size")
			break
		}
	}

	return syncImages, sizeCount, nil
}

func reduce(images []OS, sizeCount int64) ([]OS, int64, error) {
	groups := map[string][]OS{}
	for _, img := range images {
		key := fmt.Sprintf("%s-%d.%d", img.Name, img.Version.Major(), img.Version.Minor())
		groups[key] = append(groups[key], img)
	}

	var biggestGroup string
	currentBiggest := 1
	var groupNames []string
	for g := range groups {
		groupNames = append(groupNames, g)
	}
	sort.Strings(groupNames)
	for _, name := range groupNames {
		amount := len(groups[name])
		if amount > c.MinImagesPerName && amount > currentBiggest {
			currentBiggest = amount
			biggestGroup = name
		}
	}

	if biggestGroup == "" {
		return images, sizeCount, fmt.Errorf("can not reduce any further")
	}

	groupImages := groups[biggestGroup]
	groups[biggestGroup] = append([]OS{}, groupImages[1:]...)

	newSize := sizeCount - *groupImages[0].ImageRef.Size

	var result []OS
	for _, imgs := range groups {
		result = append(result, imgs...)
	}

	sortOSImagesByName(result)

	return result, newSize, nil
}

func retrieveImagesFromS3() (map[string]s3.Object, error) {
	dummyRegion := "dummy"
	s, err := session.NewSession(&aws.Config{
		Endpoint:    &c.ImageStore,
		Region:      &dummyRegion,
		Credentials: credentials.AnonymousCredentials,
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot create s3 client")
	}

	client := s3.New(s)

	objects, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: &c.ImageBucket,
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot list s3 objects")
	}

	res := map[string]s3.Object{}
	for _, o := range objects.Contents {
		res[*o.Key] = *o
	}

	return res, nil
}

func sortOSImagesByName(imgs []OS) {
	sort.Slice(imgs, func(i, j int) bool {
		if imgs[i].Name == imgs[j].Name {
			return imgs[i].Version.LessThan(imgs[j].Version)
		}
		return strings.Compare(imgs[i].Name, imgs[j].Name) < 0
	})
}

func download(img []OS) error {
	logger.Infow("download not yet implemented")
	return nil
}

func cleanup(img []OS) error {
	logger.Infow("cleanup not yet implemented")
	return nil
}

// COPIED FROM METAL-API
//
// GetOsAndSemver parses a imageID to OS and Semver, or returns an error
// the last part must be the semantic version, valid ids are:
// ubuntu-19.04                 os: ubuntu version: 19.04
// ubuntu-19.04.20200408        os: ubuntu version: 19.04.20200408
// ubuntu-small-19.04.20200408  os: ubuntu-small version: 19.04.20200408
func GetOsAndSemver(id string) (string, *semver.Version, error) {
	imageParts := strings.Split(id, "-")
	if len(imageParts) < 2 {
		return "", nil, fmt.Errorf("image does not contain a version")
	}

	parts := len(imageParts) - 1
	os := strings.Join(imageParts[:parts], "-")
	version := strings.Join(imageParts[parts:], "")
	v, err := semver.NewVersion(version)
	if err != nil {
		return "", nil, err
	}
	return os, v, nil
}
