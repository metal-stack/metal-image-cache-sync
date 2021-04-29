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
	ImageCacheRootPath    string `validate:"required"`
	ImageCacheBindAddress string `validate:"required"`

	KernelCacheEnabled     bool `validate:"required"`
	KernelCacheBindAddress string
	KernelCacheRootPath    string

	BootImageCacheEnabled     bool `validate:"required"`
	BootImageCacheBindAddress string
	BootImageCacheRootPath    string

	MinImagesPerName int   `validate:"required"`
	MaxImagesPerName int   `validate:"required"`
	MaxCacheSize     int64 `validate:"required"`

	ImageStore  string `validate:"required"`
	ImageBucket string `validate:"required"`

	MetalAPIEndpoint string `validate:"required"`
	MetalAPIHMAC     string `validate:"required"`

	SyncSchedule string `validate:"required"`
	DryRun       bool
	ExcludePaths []string

	ExpirationGraceDays uint
}

func NewConfig() (*Config, error) {
	c := &Config{
		ImageCacheBindAddress: viper.GetString("image-cache-bind-address"),
		ImageCacheRootPath:    viper.GetString("image-cache-path"),

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

		KernelCacheEnabled:     viper.GetBool("enable-kernel-cache"),
		KernelCacheRootPath:    viper.GetString("kernel-cache-path"),
		KernelCacheBindAddress: viper.GetString("kernel-cache-bind-address"),

		BootImageCacheEnabled:     viper.GetBool("enable-boot-image-cache"),
		BootImageCacheRootPath:    viper.GetString("boot-image-cache-path"),
		BootImageCacheBindAddress: viper.GetString("boot-image-cache-bind-address"),
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

	if c.KernelCacheEnabled {
		if c.KernelCacheBindAddress == "" {
			return fmt.Errorf("kernel cache bind address must be set")
		}

		if c.KernelCacheRootPath == "" {
			return fmt.Errorf("kernel cache root path must be set")
		}

		isDir, err := afero.IsDir(fs, c.KernelCacheRootPath)
		if err != nil {
			return errors.Wrap(err, "cannot open kernel cache root path")
		}
		if !isDir {
			return fmt.Errorf("kernel cache root path is not a directory")
		}
		rootPaths = append(rootPaths, c.KernelCacheRootPath)
	}

	if c.BootImageCacheEnabled {
		if c.BootImageCacheBindAddress == "" {
			return fmt.Errorf("boot image cache bind address must be set")
		}

		if c.BootImageCacheRootPath == "" {
			return fmt.Errorf("boot image cache root path must be set")
		}

		isDir, err := afero.IsDir(fs, c.BootImageCacheRootPath)
		if err != nil {
			return errors.Wrap(err, "cannot open boot image cache root path")
		}
		if !isDir {
			return fmt.Errorf("boot image cache root path is not a directory")
		}
		rootPaths = append(rootPaths, c.BootImageCacheRootPath)
	}

	rootPathMap := map[string]bool{}
	for _, s := range rootPaths {
		rootPathMap[s] = true
	}
	if len(rootPathMap) != len(rootPaths) {
		return fmt.Errorf("root paths are not disjunct: %v", rootPaths)
	}

	return nil
}
