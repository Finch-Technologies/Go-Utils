package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/finch-technologies/go-utils/log"
)

type DeleteOptions struct {
	Recursive bool
	DeleteDir bool
}

type LocalStorageOptions struct {
	BasePath string
}

type LocalStorage struct {
	BasePath string
}

func Init(options ...LocalStorageOptions) (*LocalStorage, error) {
	wd, err := os.Getwd()

	basePath := wd + "/.storage"

	if len(options) > 0 {
		basePath = options[0].BasePath
	}

	if err != nil {
		return nil, err
	}

	return &LocalStorage{BasePath: basePath}, nil
}

func (s *LocalStorage) Read(ctx context.Context, path string) ([]byte, error) {
	sourceFile, err := os.Open(s.getPath(path))
	if err != nil {
		return nil, fmt.Errorf("failed to open source file %q, %v", path, err)
	}
	defer func(sourceFile *os.File) {
		err := sourceFile.Close()
		if err != nil {
			log.Error("failed to write file %q: %v", path, err)
		}
	}(sourceFile)

	return io.ReadAll(sourceFile)
}

func (s *LocalStorage) Write(ctx context.Context, file []byte, path string) (string, error) {
	filePath := s.getPath(path)

	// split dir and file name based on the last "/"
	dir := filePath[:strings.LastIndex(filePath, "/")]

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

// DeleteFile removes a file from storage
func (s *LocalStorage) Delete(path string, options ...DeleteOptions) error {
	opts := DeleteOptions{
		Recursive: false,
		DeleteDir: false,
	}

	if len(options) > 0 {
		if options[0].Recursive {
			opts.Recursive = true
		}

		if options[0].DeleteDir {
			opts.DeleteDir = true
		}
	}

	filePath := s.getPath(path)

	// split dir and file name based on the last "/"
	dir := filePath[:strings.LastIndex(filePath, "/")]

	if opts.Recursive {
		if err := os.RemoveAll(filePath); err != nil {
			return fmt.Errorf("failed to delete directory %s: %w", filePath, err)
		}
	} else {
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("failed to delete file %s: %w", filePath, err)
		}
	}

	// delete the directory if there are no files in it
	if opts.DeleteDir {
		//Check if the directory is empty
		files, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read directory %s: %w", dir, err)
		}
		if len(files) == 0 {
			if err := os.RemoveAll(dir); err != nil {
				return fmt.Errorf("failed to delete directory %s: %w", dir, err)
			}
		}
	}

	return nil
}

// FileExists checks if a file exists
func (s *LocalStorage) FileExists(path string) bool {
	_, err := os.Stat(s.getPath(path))
	return !os.IsNotExist(err)
}

// GetFileSize returns the size of a file
func (s *LocalStorage) GetFileSize(path string) (int64, error) {
	info, err := os.Stat(s.getPath(path))
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for %s: %w", s.getPath(path), err)
	}
	return info.Size(), nil
}

func (s *LocalStorage) getPath(path string) string {
	if s.BasePath == "" {
		return path
	}

	// If path starts with a slash, assume its an absolute path
	if strings.HasPrefix(path, "/") || strings.HasPrefix(path, "./") || strings.HasPrefix(path, "~") || strings.HasPrefix(path, "../") {
		return path
	}

	//remove trailing slash from base path
	basePath := strings.TrimSuffix(s.BasePath, "/")

	return basePath + "/" + path
}
