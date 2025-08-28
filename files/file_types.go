package files

import (
	"context"

	"github.com/finch-technologies/go-utils/files/types"
)

type Storage interface {
	Upload(ctx context.Context, file []byte, key string, options ...types.UploadOptions) (string, error)
	Download(ctx context.Context, key, filePath string) error
}
