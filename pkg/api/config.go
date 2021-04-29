package api

import (
	"fmt"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/go-playground/validator.v9"
)

type Config struct {
	ImageCacheRootPath  string `validate:"required"`
	ImageCacheServePath string `validate:"required"`

	MinImagesPerName int   `validate:"required"`
	MaxImagesPerName int   `validate:"required"`
	MaxCacheSize     int64 `validate:"required"`

	ImageStore  string `validate:"required"`
	ImageBucket string `validate:"required"`

	MetalAPIEndpoint string `validate:"required"`
	MetalAPIHMAC     string `validate:"required"`

	BindAddress  string `validate:"required"`
	SyncSchedule string `validate:"required"`
	DryRun       bool
	ExcludePaths []string

	ExpirationGraceDays uint

	KernelCacheEnabled   bool   `validate:"required"`
	KernelCacheRootPath  string `validate:"required"`
	KernelCacheServePath string `validate:"required"`

	BootImageCacheEnabled   bool   `validate:"required"`
	BootImageCacheRootPath  string `validate:"required"`
	BootImageCacheServePath string `validate:"required"`
}

func NewConfig() (*Config, error) {
	c := &Config{
		BindAddress:         viper.GetString("bind-address"),
		MinImagesPerName:    viper.GetInt("min-images-per-name"),
		MaxImagesPerName:    viper.GetInt("max-images-per-name"),
		ImageStore:          viper.GetString("image-store"),
		ImageBucket:         viper.GetString("image-store-bucket"),
		MetalAPIEndpoint:    viper.GetString("metal-api-endpoint"),
		MetalAPIHMAC:        viper.GetString("metal-api-hmac"),
		SyncSchedule:        viper.GetString("schedule"),
		DryRun:              viper.GetBool("dry-run"),
		ExcludePaths:        viper.GetStringSlice("excludes"),
		ExpirationGraceDays: viper.GetUint("expiration-grace-period"),

		ImageCacheRootPath:  viper.GetString("image-cache-path"),
		ImageCacheServePath: viper.GetString("image-cache-serve-path"),

		KernelCacheEnabled:   viper.GetBool("enable-kernel-cache"),
		KernelCacheRootPath:  viper.GetString("kernel-cache-path"),
		KernelCacheServePath: viper.GetString("kernel-cache-serve-path"),

		BootImageCacheEnabled:   viper.GetBool("enable-boot-image-cache"),
		BootImageCacheRootPath:  viper.GetString("boot-image-cache-path"),
		BootImageCacheServePath: viper.GetString("boot-image-cache-serve-path"),
	}

	var err error
	c.MaxCacheSize, err = units.FromHumanSize(viper.GetString("max-cache-size"))
	if err != nil {
		return nil, errors.Wrap(err, "cannot read max cache size")
	}

	return c, nil
}

func (c *Config) Validate(fs afero.Fs) error {
	validate := validator.New()
	err := validate.Struct(c)
	if err != nil {
		return err
	}

	isDir, err := afero.IsDir(fs, c.ImageCacheRootPath)
	if err != nil {
		return errors.Wrap(err, "cannot open cache root path")
	}
	if !isDir {
		return fmt.Errorf("image cache root path is not a directory")
	}

	if c.MinImagesPerName < 1 {
		return fmt.Errorf("minimum images per name must be at least 1")
	}

	rootPaths := []string{c.ImageCacheRootPath}
	servePaths := []string{c.ImageCacheServePath}

	if c.KernelCacheEnabled {
		isDir, err := afero.IsDir(fs, c.KernelCacheRootPath)
		if err != nil {
			return errors.Wrap(err, "cannot open kernel cache root path")
		}
		if !isDir {
			return fmt.Errorf("kernel cache root path is not a directory")
		}
		rootPaths = append(rootPaths, c.KernelCacheRootPath)
		servePaths = append(servePaths, c.KernelCacheServePath)
	}

	if c.BootImageCacheEnabled {
		isDir, err := afero.IsDir(fs, c.BootImageCacheRootPath)
		if err != nil {
			return errors.Wrap(err, "cannot open boot image cache root path")
		}
		if !isDir {
			return fmt.Errorf("boot image cache root path is not a directory")
		}
		rootPaths = append(rootPaths, c.BootImageCacheRootPath)
		servePaths = append(servePaths, c.BootImageCacheServePath)
	}

	rootPathMap := map[string]bool{}
	for _, s := range rootPaths {
		rootPathMap[s] = true
	}
	if len(rootPathMap) != len(rootPaths) {
		return fmt.Errorf("root paths are not disjunct: %v", rootPaths)
	}

	servePathMap := map[string]bool{}
	for _, s := range servePaths {
		servePathMap[s] = true
	}
	if len(servePathMap) != len(servePaths) {
		return fmt.Errorf("serve paths are not disjunct: %v", servePaths)
	}

	return nil
}
