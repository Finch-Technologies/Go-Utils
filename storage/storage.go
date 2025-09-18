package storage

import (
	"context"
	"fmt"

	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/storage/filesystem"
	"github.com/finch-technologies/go-utils/storage/s3"
	"github.com/finch-technologies/go-utils/storage/types"
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
	Type   StorageType
	Bucket string
	Region string
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

	switch cfg.Type {
	case StorageDiskS3:
		log.Debugf("Using S3 storage: %s", cfg.Bucket)
		storage, err = s3.GetS3Storage(cfg.Bucket, cfg.Region)
	default:
		log.Debugf("Using local storage: %s", cfg.Type)
		storage, err = filesystem.GetLocalStorage()
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

func (fm *FileManager) Download(ctx context.Context, key, filePath string) error {
	return fm.storage.Download(ctx, key, filePath)
}
