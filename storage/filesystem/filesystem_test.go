package filesystem

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInit(t *testing.T) {
	tests := []struct {
		name        string
		options     []LocalStorageOptions
		expectError bool
	}{
		{
			name:        "default initialization",
			options:     nil,
			expectError: false,
		},
		{
			name: "custom base path",
			options: []LocalStorageOptions{
				{BasePath: "/tmp/test-storage"},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, err := Init(tt.options...)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if storage == nil {
				t.Error("expected non-nil storage")
				return
			}

			if tt.options == nil {
				wd, _ := os.Getwd()
				expectedPath := wd + "/.storage"
				if storage.BasePath != expectedPath {
					t.Errorf("expected BasePath %s, got %s", expectedPath, storage.BasePath)
				}
			} else {
				if storage.BasePath != tt.options[0].BasePath {
					t.Errorf("expected BasePath %s, got %s", tt.options[0].BasePath, storage.BasePath)
				}
			}
		})
	}
}

func TestReadWrite(t *testing.T) {
	// Create temp directory for testing
	tempDir := t.TempDir()
	storage := &LocalStorage{BasePath: tempDir}
	ctx := context.Background()

	tests := []struct {
		name     string
		path     string
		content  []byte
		readPath string
	}{
		{
			name:     "simple file",
			path:     "test.txt",
			content:  []byte("hello world"),
			readPath: "test.txt",
		},
		{
			name:     "nested directory",
			path:     "nested/dir/test.txt",
			content:  []byte("nested content"),
			readPath: "nested/dir/test.txt",
		},
		{
			name:     "empty file",
			path:     "empty.txt",
			content:  []byte(""),
			readPath: "empty.txt",
		},
		{
			name:     "binary content",
			path:     "binary.bin",
			content:  []byte{0x00, 0x01, 0x02, 0xFF},
			readPath: "binary.bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Write
			_, err := storage.Write(ctx, tt.content, tt.path)
			if err != nil {
				t.Errorf("Write failed: %v", err)
				return
			}

			// Test Read
			readContent, err := storage.Read(ctx, tt.readPath)
			if err != nil {
				t.Errorf("Read failed: %v", err)
				return
			}

			// Compare content
			if len(readContent) != len(tt.content) {
				t.Errorf("content length mismatch: expected %d, got %d", len(tt.content), len(readContent))
				return
			}

			for i, b := range tt.content {
				if readContent[i] != b {
					t.Errorf("content mismatch at byte %d: expected %d, got %d", i, b, readContent[i])
					break
				}
			}
		})
	}
}

func TestFileExists(t *testing.T) {
	tempDir := t.TempDir()
	storage := &LocalStorage{BasePath: tempDir}
	ctx := context.Background()

	// Create a test file
	testContent := []byte("test content")
	_, err := storage.Write(ctx, testContent, "exists.txt")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "existing file",
			path:     "exists.txt",
			expected: true,
		},
		{
			name:     "non-existing file",
			path:     "not-exists.txt",
			expected: false,
		},
		{
			name:     "nested non-existing",
			path:     "nested/not-exists.txt",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exists := storage.FileExists(tt.path)
			if exists != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, exists)
			}
		})
	}
}

func TestGetFileSize(t *testing.T) {
	tempDir := t.TempDir()
	storage := &LocalStorage{BasePath: tempDir}
	ctx := context.Background()

	tests := []struct {
		name         string
		content      []byte
		path         string
		expectedSize int64
	}{
		{
			name:         "empty file",
			content:      []byte(""),
			path:         "empty.txt",
			expectedSize: 0,
		},
		{
			name:         "small file",
			content:      []byte("hello"),
			path:         "small.txt",
			expectedSize: 5,
		},
		{
			name:         "larger file",
			content:      []byte(strings.Repeat("a", 1000)),
			path:         "large.txt",
			expectedSize: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create file
			_, err := storage.Write(ctx, tt.content, tt.path)
			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			// Get size
			size, err := storage.GetFileSize(tt.path)
			if err != nil {
				t.Errorf("GetFileSize failed: %v", err)
				return
			}

			if size != tt.expectedSize {
				t.Errorf("expected size %d, got %d", tt.expectedSize, size)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tempDir := t.TempDir()
	storage := &LocalStorage{BasePath: tempDir}
	ctx := context.Background()

	// Create test files
	testContent := []byte("test content")
	_, err := storage.Write(ctx, testContent, "delete-me.txt")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	_, err = storage.Write(ctx, testContent, "nested/delete-me.txt")
	if err != nil {
		t.Fatalf("failed to create nested test file: %v", err)
	}

	tests := []struct {
		name        string
		path        string
		expectError bool
	}{
		{
			name:        "delete existing file",
			path:        "delete-me.txt",
			expectError: false,
		},
		{
			name:        "delete nested file",
			path:        "nested/delete-me.txt",
			expectError: false,
		},
		{
			name:        "delete non-existing file",
			path:        "not-exists.txt",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.Delete(tt.path)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}

				// Verify file no longer exists
				if storage.FileExists(tt.path) {
					t.Error("file still exists after deletion")
				}
			}
		})
	}
}

func TestGetPath(t *testing.T) {
	tests := []struct {
		name        string
		basePath    string
		inputPath   string
		expectedEnd string
	}{
		{
			name:        "relative path with base",
			basePath:    "/tmp/storage",
			inputPath:   "file.txt",
			expectedEnd: "/tmp/storage/file.txt",
		},
		{
			name:        "absolute path ignores base",
			basePath:    "/tmp/storage",
			inputPath:   "/absolute/file.txt",
			expectedEnd: "/absolute/file.txt",
		},
		{
			name:        "current dir path ignores base",
			basePath:    "/tmp/storage",
			inputPath:   "./file.txt",
			expectedEnd: "./file.txt",
		},
		{
			name:        "home path ignores base",
			basePath:    "/tmp/storage",
			inputPath:   "~/file.txt",
			expectedEnd: "~/file.txt",
		},
		{
			name:        "parent dir path ignores base",
			basePath:    "/tmp/storage",
			inputPath:   "../file.txt",
			expectedEnd: "../file.txt",
		},
		{
			name:        "empty base path",
			basePath:    "",
			inputPath:   "file.txt",
			expectedEnd: "file.txt",
		},
		{
			name:        "base path with trailing slash",
			basePath:    "/tmp/storage/",
			inputPath:   "file.txt",
			expectedEnd: "/tmp/storage/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := &LocalStorage{BasePath: tt.basePath}
			result := storage.getPath(tt.inputPath)

			if result != tt.expectedEnd {
				t.Errorf("expected %s, got %s", tt.expectedEnd, result)
			}
		})
	}
}

func TestEdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	storage := &LocalStorage{BasePath: tempDir}
	ctx := context.Background()

	t.Run("read non-existent file", func(t *testing.T) {
		_, err := storage.Read(ctx, "non-existent.txt")
		if err == nil {
			t.Error("expected error when reading non-existent file")
		}
	})

	t.Run("get size of non-existent file", func(t *testing.T) {
		_, err := storage.GetFileSize("non-existent.txt")
		if err == nil {
			t.Error("expected error when getting size of non-existent file")
		}
	})

	t.Run("write to deeply nested path", func(t *testing.T) {
		deepPath := "a/b/c/d/e/f/g/deep.txt"
		content := []byte("deep content")

		_, err := storage.Write(ctx, content, deepPath)
		if err != nil {
			t.Errorf("failed to write to deep path: %v", err)
			return
		}

		// Verify it was written
		readContent, err := storage.Read(ctx, deepPath)
		if err != nil {
			t.Errorf("failed to read deep path: %v", err)
			return
		}

		if string(readContent) != string(content) {
			t.Error("deep path content mismatch")
		}
	})

	t.Run("very large file", func(t *testing.T) {
		// Create 1MB file
		largeContent := make([]byte, 1024*1024)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		_, err := storage.Write(ctx, largeContent, "large.bin")
		if err != nil {
			t.Errorf("failed to write large file: %v", err)
			return
		}

		readContent, err := storage.Read(ctx, "large.bin")
		if err != nil {
			t.Errorf("failed to read large file: %v", err)
			return
		}

		if len(readContent) != len(largeContent) {
			t.Errorf("large file size mismatch: expected %d, got %d", len(largeContent), len(readContent))
		}

		size, err := storage.GetFileSize("large.bin")
		if err != nil {
			t.Errorf("failed to get large file size: %v", err)
		} else if size != int64(len(largeContent)) {
			t.Errorf("large file size mismatch: expected %d, got %d", len(largeContent), size)
		}
	})

	t.Run("special characters in filename", func(t *testing.T) {
		specialPath := "special file !@#$%^&()_+-=.txt"
		content := []byte("special content")

		_, err := storage.Write(ctx, content, specialPath)
		if err != nil {
			t.Errorf("failed to write file with special chars: %v", err)
			return
		}

		exists := storage.FileExists(specialPath)
		if !exists {
			t.Error("file with special chars doesn't exist")
		}
	})
}

func TestConcurrentOperations(t *testing.T) {
	tempDir := t.TempDir()
	storage := &LocalStorage{BasePath: tempDir}
	ctx := context.Background()

	// Test concurrent writes to different files
	t.Run("concurrent writes", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(id int) {
				defer func() { done <- true }()

				content := []byte(strings.Repeat("x", 100))
				path := filepath.Join("concurrent", "file_"+string(rune('0'+id))+".txt")

				_, err := storage.Write(ctx, content, path)
				if err != nil {
					t.Errorf("concurrent write %d failed: %v", id, err)
				}
			}(i)
		}

		// Wait for all writes to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		// Verify all files exist
		for i := 0; i < 10; i++ {
			path := filepath.Join("concurrent", "file_"+string(rune('0'+i))+".txt")
			if !storage.FileExists(path) {
				t.Errorf("concurrent file %d doesn't exist", i)
			}
		}
	})
}

func TestWrite(t *testing.T) {
	storage, err := Init()
	if err != nil {
		t.Errorf("failed to initialize storage: %v", err)
	}

	ctx := context.Background()

	content := []byte("test content")
	_, err = storage.Write(ctx, content, "test.txt")
	if err != nil {
		t.Errorf("failed to write file: %v", err)
	}

	exists := storage.FileExists("test.txt")
	if !exists {
		t.Error("file doesn't exist")
	}

	readContent, err := storage.Read(ctx, "test.txt")
	if err != nil {
		t.Errorf("failed to read file: %v", err)
	}

	if string(readContent) != string(content) {
		t.Error("file content mismatch")
	}

	err = storage.Delete("test.txt", DeleteOptions{
		DeleteDir: true,
	})
	if err != nil {
		t.Errorf("failed to delete file: %v", err)
	}
}
