package storage

import (
	"context"

	"github.com/finch-technologies/go-utils/storage/types"
)

type Storage interface {
	Upload(ctx context.Context, file []byte, key string, options ...types.UploadOptions) (string, error)
	Download(ctx context.Context, key string) ([]byte, error)
}
