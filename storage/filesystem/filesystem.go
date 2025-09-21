package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/storage/types"
)

type LocalStorage struct {
	BasePath string
}

func GetLocalStorage() (*LocalStorage, error) {
	basePath, err := os.Getwd()

	if err != nil {
		return nil, err
	}

	return &LocalStorage{BasePath: basePath + "/.storage"}, nil
}

func (s *LocalStorage) Upload(ctx context.Context, file []byte, dirAndFileName string, options ...types.UploadOptions) (string, error) {
	filePath := s.BasePath + "/" + dirAndFileName

	// split dir and file name based on the last "/"
	dir := s.BasePath + "/" + dirAndFileName[:strings.LastIndex(dirAndFileName, "/")]

	// Create the directory if it doesn't exist
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to write directory %q: %v", s.BasePath, err)
	}

	err = os.WriteFile(filePath, file, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to write file %q: %v", filePath, err)
	}
	return "", nil
}

func (s *LocalStorage) Download(ctx context.Context, key string) ([]byte, error) {
	sourceFile, err := os.Open(s.BasePath + "/" + key)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file %q, %v", key, err)
	}
	defer func(sourceFile *os.File) {
		err := sourceFile.Close()
		if err != nil {
			log.Error("failed to write file %q: %v", key, err)
		}
	}(sourceFile)

	return io.ReadAll(sourceFile)
}
