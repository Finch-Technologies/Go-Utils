package s3

import (
	"errors"
	"os"
	"time"

	"github.com/finch-technologies/go-utils/utils"
)

func getConfig(config ...S3Config) (*S3Config, error) {
	defaultConfig := S3Config{
		Region: utils.StringOrDefault(os.Getenv("S3_REGION"), "af-south-1"),
	}

	if len(config) == 0 {
		return nil, errors.New("no config provided")
	}

	cfg := defaultConfig

	if len(config) > 0 {
		cfg = config[0]

		if cfg.Bucket == "" {
			return nil, errors.New("bucket is required")
		}

		utils.MergeObjects(&cfg, defaultConfig)
	}

	return &cfg, nil
}

func getUploadOptions(options ...UploadOptions) UploadOptions {
	defaultOptions := UploadOptions{
		ReturnType:      S3ReturnTypeKey,
		PresignedUrlTTL: 30 * time.Minute,
		Metadata:        map[string]string{},
	}

	if len(options) == 0 {
		return defaultOptions
	}

	opts := options[0]

	// Apply defaults for zero values manually to avoid MergeObjects issue with maps
	if opts.ReturnType == "" {
		opts.ReturnType = defaultOptions.ReturnType
	}
	if opts.PresignedUrlTTL == 0 {
		opts.PresignedUrlTTL = defaultOptions.PresignedUrlTTL
	}
	if opts.Metadata == nil {
		opts.Metadata = defaultOptions.Metadata
	}

	return opts
}
