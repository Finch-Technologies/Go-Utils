package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/storage/filesystem"
	"github.com/finch-technologies/go-utils/storage/s3"
	"github.com/finch-technologies/go-utils/storage/types"
	"github.com/finch-technologies/go-utils/utils"
)

type FileManager struct {
	storage   Storage
	initError bool
}

type StorageType string

const (
	StorageDiskLocal StorageType = "local"
	StorageDiskS3    StorageType = "s3"
)

type StorageConfig struct {
	Type      StorageType
	Bucket    string
	Region    string
	KeyPrefix string
}

func getConfig(config ...StorageConfig) StorageConfig {
	if len(config) == 0 {
		return StorageConfig{Type: StorageDiskLocal}
	}
	return config[0]
}

var fm *FileManager

func Init(config ...StorageConfig) (*FileManager, error) {
	var storage Storage
	var err error

	cfg := getConfig(config...)

	if cfg.Region == "" {
		cfg.Region = utils.StringOrDefault(os.Getenv("AWS_REGION"), "af-south-1")
	}

	switch cfg.Type {
	case StorageDiskS3:
		if cfg.Bucket == "" {
			return nil, fmt.Errorf("s3 bucket is required")
		}
		log.Debugf("Using S3 storage: %s", cfg.Bucket)
		storage, err = s3.New(s3.S3Config{
			Bucket:    cfg.Bucket,
			Region:    cfg.Region,
			KeyPrefix: cfg.KeyPrefix,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create s3 storage: %s", err)
		}
	default:
		log.Debugf("Using local storage: %s", cfg.Type)
		storage, err = filesystem.GetLocalStorage()
		if err != nil {
			return nil, fmt.Errorf("failed to create local storage: %s", err)
		}
	}

	if err != nil {
		fm = &FileManager{initError: true}
		return nil, err
	}

	fm = &FileManager{storage: storage, initError: false}

	return fm, nil
}

func GetStorage() (Storage, error) {
	if fm == nil {
		return Init()
	}

	if fm.initError {
		return nil, fmt.Errorf("failed to initialize file manager")
	}

	return fm.storage, nil
}

func (fm *FileManager) Upload(ctx context.Context, file []byte, key string, options ...types.UploadOptions) (string, error) {
	uploaded, err := fm.storage.Upload(ctx, file, key, options...)
	return uploaded, err
}

func (fm *FileManager) Download(ctx context.Context, key string) ([]byte, error) {
	return fm.storage.Download(ctx, key)
}
