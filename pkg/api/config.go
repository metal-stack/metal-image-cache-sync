package api

import (
	"fmt"
	"path"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"gopkg.in/go-playground/validator.v9"
)

type Config struct {
	CacheRootPath string `validate:"required"`

	KernelCacheEnabled    bool `validate:"required"`
	BootImageCacheEnabled bool `validate:"required"`

	ImageCacheBindAddress     string `validate:"required"`
	KernelCacheBindAddress    string
	BootImageCacheBindAddress string

	MetalAPIEndpoint string `validate:"required"`
	MetalAPIHMAC     string `validate:"required"`

	SyncSchedule string `validate:"required"`
	DryRun       bool
	ExcludePaths []string

	// OS Image related settings

	MinImagesPerName int   `validate:"required"`
	MaxImagesPerName int   `validate:"required"`
	MaxCacheSize     int64 `validate:"required"`

	ImageStore  string `validate:"required"`
	ImageBucket string `validate:"required"`

	ExpirationGraceDays uint
}

func NewConfig() (*Config, error) {
	c := &Config{
		CacheRootPath:             viper.GetString("cache-root-path"),
		KernelCacheEnabled:        viper.GetBool("enable-kernel-cache"),
		BootImageCacheEnabled:     viper.GetBool("enable-boot-image-cache"),
		ImageCacheBindAddress:     viper.GetString("image-cache-bind-address"),
		MetalAPIEndpoint:          viper.GetString("metal-api-endpoint"),
		MetalAPIHMAC:              viper.GetString("metal-api-hmac"),
		BootImageCacheBindAddress: viper.GetString("boot-image-cache-bind-address"),
		KernelCacheBindAddress:    viper.GetString("kernel-cache-bind-address"),
		MinImagesPerName:          viper.GetInt("min-images-per-name"),
		MaxImagesPerName:          viper.GetInt("max-images-per-name"),
		ImageStore:                viper.GetString("image-store"),
		ImageBucket:               viper.GetString("image-store-bucket"),
		SyncSchedule:              viper.GetString("schedule"),
		DryRun:                    viper.GetBool("dry-run"),
		ExcludePaths:              viper.GetStringSlice("excludes"),
		ExpirationGraceDays:       viper.GetUint("expiration-grace-period"),
	}

	var err error
	c.MaxCacheSize, err = units.FromHumanSize(viper.GetString("max-cache-size"))
	if err != nil {
		return nil, errors.Wrap(err, "cannot read max cache size")
	}

	return c, nil
}

func (c *Config) GetImageRootPath() string {
	return path.Join(c.CacheRootPath, "images")
}

func (c *Config) GetTmpDownloadPath() string {
	return path.Join(c.CacheRootPath, "tmp")
}

func (c *Config) GetKernelRootPath() string {
	return path.Join(c.CacheRootPath, "kernels")
}

func (c *Config) GetBootImageRootPath() string {
	return path.Join(c.CacheRootPath, "boot-images")
}

func (c *Config) Validate(fs afero.Fs) error {
	validate := validator.New()
	err := validate.Struct(c)
	if err != nil {
		return err
	}

	isDir, err := afero.IsDir(fs, c.CacheRootPath)
	if err != nil {
		return errors.Wrap(err, "cannot open cache root path")
	}
	if !isDir {
		return fmt.Errorf("cache root path is not a directory")
	}

	if c.MinImagesPerName < 1 {
		return fmt.Errorf("minimum images per name must be at least 1")
	}

	if c.KernelCacheEnabled {
		if c.KernelCacheBindAddress == "" {
			return fmt.Errorf("kernel cache bind address must be set")
		}
	}

	if c.BootImageCacheEnabled {
		if c.BootImageCacheBindAddress == "" {
			return fmt.Errorf("boot image cache bind address must be set")
		}
	}

	return nil
}
