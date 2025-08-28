package files

import (
	"context"
	"fmt"
	"os"

	local "github.com/finch-technologies/go-utils/files/local"
	s3 "github.com/finch-technologies/go-utils/files/s3"
	"github.com/finch-technologies/go-utils/files/types"
	"github.com/finch-technologies/go-utils/log"
)

type FileManager struct {
	storage   Storage
	initError bool
}

var fm *FileManager

func Init() (*FileManager, error) {
	var storage Storage
	var err error
	storageType := os.Getenv("STORAGE_TYPE")

	switch storageType {
	case "s3":
		log.Debug("Using S3 storage")
		region := os.Getenv("AWS_REGION")
		bucket := os.Getenv("AWS_BUCKET_NAME")
		storage, err = s3.GetS3Storage(bucket, region)
	default:
		log.Debug("Using local storage")
		storage, err = local.GetLocalStorage()
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
