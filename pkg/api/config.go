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
	ImageCacheRootPath string `validate:"required"`

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

	ExpirationGraceDays int
}

func NewConfig() (*Config, error) {
	c := &Config{
		BindAddress:         viper.GetString("bind-address"),
		ImageCacheRootPath:  viper.GetString("root-path"),
		MinImagesPerName:    viper.GetInt("min-images-per-name"),
		MaxImagesPerName:    viper.GetInt("max-images-per-name"),
		ImageStore:          viper.GetString("image-store"),
		ImageBucket:         viper.GetString("image-store-bucket"),
		MetalAPIEndpoint:    viper.GetString("metal-api-endpoint"),
		MetalAPIHMAC:        viper.GetString("metal-api-hmac"),
		SyncSchedule:        viper.GetString("schedule"),
		DryRun:              viper.GetBool("dry-run"),
		ExcludePaths:        viper.GetStringSlice("excludes"),
		ExpirationGraceDays: viper.GetInt("expiration-grace-period"),
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

	if c.ExpirationGraceDays < 0 {
		return fmt.Errorf("expiration grace period must be >= 0")
	}

	return nil
}
