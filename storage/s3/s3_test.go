package s3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

var testBucket = "go-utils.test"
var testRegion = "af-south-1"

func TestNew(t *testing.T) {

	tests := []struct {
		name        string
		config      []S3Config
		expectError bool
	}{
		{
			name:        "no config provided",
			config:      []S3Config{},
			expectError: true,
		},
		{
			name: "valid config",
			config: []S3Config{
				{
					Bucket:    testBucket,
					Region:    testRegion,
					KeyPrefix: "test-prefix",
				},
			},
			expectError: false,
		},
		{
			name: "config without bucket",
			config: []S3Config{
				{
					Region:    testRegion,
					KeyPrefix: "test-prefix",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(tt.config...)

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

			if client == nil {
				t.Error("expected non-nil client")
				return
			}

			if len(tt.config) > 0 {
				cfg := tt.config[0]
				if client.Bucket != cfg.Bucket {
					t.Errorf("expected bucket %s, got %s", cfg.Bucket, client.Bucket)
				}
				if client.Region != cfg.Region {
					t.Errorf("expected region %s, got %s", cfg.Region, client.Region)
				}
				if client.KeyPrefix != cfg.KeyPrefix {
					t.Errorf("expected keyPrefix %s, got %s", cfg.KeyPrefix, client.KeyPrefix)
				}
			}
		})
	}
}

func TestS3Config(t *testing.T) {
	tests := []struct {
		name        string
		config      []S3Config
		expectError bool
		expected    *S3Config
	}{
		{
			name:        "no config",
			config:      []S3Config{},
			expectError: true,
			expected:    nil,
		},
		{
			name: "valid config",
			config: []S3Config{
				{
					Bucket:    testBucket,
					Region:    testRegion,
					KeyPrefix: "uploads",
				},
			},
			expectError: false,
			expected: &S3Config{
				Bucket:    testBucket,
				Region:    testRegion,
				KeyPrefix: "uploads",
			},
		},
		{
			name: "config without bucket",
			config: []S3Config{
				{
					Region:    testRegion,
					KeyPrefix: "uploads",
				},
			},
			expectError: true,
			expected:    nil,
		},
		{
			name: "config with defaults",
			config: []S3Config{
				{
					Bucket: testBucket,
				},
			},
			expectError: false,
			expected: &S3Config{
				Bucket:    testBucket,
				Region:    testRegion,
				KeyPrefix: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := getConfig(tt.config...)

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

			if cfg.Bucket != tt.expected.Bucket {
				t.Errorf("expected bucket %s, got %s", tt.expected.Bucket, cfg.Bucket)
			}
			if cfg.Region != tt.expected.Region {
				t.Errorf("expected region %s, got %s", tt.expected.Region, cfg.Region)
			}
			if cfg.KeyPrefix != tt.expected.KeyPrefix {
				t.Errorf("expected keyPrefix %s, got %s", tt.expected.KeyPrefix, cfg.KeyPrefix)
			}
		})
	}
}

func TestUploadOptions(t *testing.T) {
	tests := []struct {
		name     string
		options  []UploadOptions
		expected UploadOptions
	}{
		{
			name:    "default options",
			options: []UploadOptions{},
			expected: UploadOptions{
				ReturnType:      S3ReturnTypeKey,
				PresignedUrlTTL: 30 * time.Minute,
				Metadata:        map[string]string{},
			},
		},
		{
			name: "custom options",
			options: []UploadOptions{
				{
					ReturnType:      S3ReturnTypePresignedUrl,
					ContentType:     "text/plain",
					FileSize:        1024,
					PresignedUrlTTL: 60 * time.Minute,
					Metadata:        map[string]string{"key": "value"},
				},
			},
			expected: UploadOptions{
				ReturnType:      S3ReturnTypePresignedUrl,
				ContentType:     "text/plain",
				FileSize:        1024,
				PresignedUrlTTL: 60 * time.Minute,
				Metadata:        map[string]string{"key": "value"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := getUploadOptions(tt.options...)

			if opts.ReturnType != tt.expected.ReturnType {
				t.Errorf("expected ReturnType %s, got %s", tt.expected.ReturnType, opts.ReturnType)
			}
			if opts.ContentType != tt.expected.ContentType {
				t.Errorf("expected ContentType %s, got %s", tt.expected.ContentType, opts.ContentType)
			}
			if opts.FileSize != tt.expected.FileSize {
				t.Errorf("expected FileSize %d, got %d", tt.expected.FileSize, opts.FileSize)
			}
			if opts.PresignedUrlTTL != tt.expected.PresignedUrlTTL {
				t.Errorf("expected PresignedUrlTTL %v, got %v", tt.expected.PresignedUrlTTL, opts.PresignedUrlTTL)
			}

			// Check metadata maps manually
			if len(opts.Metadata) != len(tt.expected.Metadata) {
				t.Errorf("expected metadata length %d, got %d", len(tt.expected.Metadata), len(opts.Metadata))
			} else {
				for key, expectedVal := range tt.expected.Metadata {
					if actualVal, exists := opts.Metadata[key]; !exists || actualVal != expectedVal {
						t.Errorf("expected metadata[%s] = %s, got %s", key, expectedVal, actualVal)
					}
				}
			}
		})
	}
}

func TestIsS3URL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{
			name:     "s3 scheme URL",
			url:      "s3://bucket/key",
			expected: true,
		},
		{
			name:     "https s3 amazonaws URL",
			url:      fmt.Sprintf("https://bucket.s3.%s.amazonaws.com/key", testRegion),
			expected: true,
		},
		{
			name:     "https s3 regional URL",
			url:      fmt.Sprintf("https://s3.%s.amazonaws.com/bucket/key", testRegion),
			expected: true,
		},
		{
			name:     "non-s3 URL",
			url:      "https://example.com/file",
			expected: false,
		},
		{
			name:     "invalid URL",
			url:      "not-a-url",
			expected: false,
		},
		{
			name:     "empty URL",
			url:      "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsS3URL(tt.url)
			if result != tt.expected {
				t.Errorf("expected %v, got %v for URL: %s", tt.expected, result, tt.url)
			}
		})
	}
}

func TestParseS3URL(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		expectedBucket string
		expectedKey    string
		expectError    bool
	}{
		{
			name:           "s3 scheme URL",
			url:            fmt.Sprintf("s3://%s/path/to/file.txt", testBucket),
			expectedBucket: testBucket,
			expectedKey:    "path/to/file.txt",
			expectError:    false,
		},
		{
			name:           "https bucket subdomain",
			url:            fmt.Sprintf("https://%s.s3.%s.amazonaws.com/path/to/file.txt", testBucket, testRegion),
			expectedBucket: testBucket,
			expectedKey:    "path/to/file.txt",
			expectError:    false,
		},
		{
			name:           "https s3 regional",
			url:            fmt.Sprintf("https://s3.%s.amazonaws.com/%s/path/to/file.txt", testRegion, testBucket),
			expectedBucket: testBucket,
			expectedKey:    "path/to/file.txt",
			expectError:    false,
		},
		{
			name:           "https s3 legacy",
			url:            fmt.Sprintf("https://s3-%s.amazonaws.com/%s/path/to/file.txt", testRegion, testBucket),
			expectedBucket: testBucket,
			expectedKey:    "path/to/file.txt",
			expectError:    false,
		},
		{
			name:        "invalid URL",
			url:         "not-a-url",
			expectError: true,
		},
		{
			name:        "non-s3 URL",
			url:         "https://example.com/file.txt",
			expectError: true,
		},
		{
			name:        "s3 URL without key",
			url:         fmt.Sprintf("s3://%s", testBucket),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParseS3URL(tt.url)

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

			if bucket != tt.expectedBucket {
				t.Errorf("expected bucket %s, got %s", tt.expectedBucket, bucket)
			}
			if key != tt.expectedKey {
				t.Errorf("expected key %s, got %s", tt.expectedKey, key)
			}
		})
	}
}

// Integration tests that require actual S3 access
func TestS3Integration(t *testing.T) {
	client, err := New(S3Config{
		Bucket:    testBucket,
		Region:    testRegion,
		KeyPrefix: "test",
	})
	if err != nil {
		t.Fatalf("failed to create S3 client: %v", err)
	}

	ctx := context.Background()
	testKey := "integration-test-file.txt"
	testContent := []byte("This is a test file for S3 integration testing")

	t.Run("upload and download", func(t *testing.T) {
		// Upload file
		result, err := client.Upload(ctx, testContent, testKey, UploadOptions{
			ReturnType:  S3ReturnTypeKey,
			ContentType: "text/plain",
		})
		if err != nil {
			t.Fatalf("failed to upload file: %v", err)
		}

		if !strings.Contains(result, testKey) {
			t.Errorf("expected result to contain key %s, got %s", testKey, result)
		}

		// Check if file exists
		exists, err := client.FileExists(ctx, testKey)
		if err != nil {
			t.Errorf("failed to check if file exists: %v", err)
		}
		if !exists {
			t.Error("file should exist after upload")
		}

		// Download file
		downloadedContent, err := client.Download(ctx, testKey)
		if err != nil {
			t.Fatalf("failed to download file: %v", err)
		}

		if string(downloadedContent) != string(testContent) {
			t.Errorf("downloaded content doesn't match uploaded content")
		}

		// Get file info
		fileInfo, err := client.GetS3FileInfo(ctx, testBucket, testKey)
		if err != nil {
			t.Errorf("failed to get file info: %v", err)
		} else {
			if fileInfo.Size != int64(len(testContent)) {
				t.Errorf("expected file size %d, got %d", len(testContent), fileInfo.Size)
			}
			if fileInfo.ContentType != "text/plain" {
				t.Errorf("expected content type text/plain, got %s", fileInfo.ContentType)
			}
		}

		// Clean up - delete file
		err = client.DeleteFile(ctx, testKey)
		if err != nil {
			t.Errorf("failed to delete file: %v", err)
		}

		// Verify file is deleted
		exists, err = client.FileExists(ctx, testKey)
		if err != nil {
			t.Errorf("failed to check if file exists after deletion: %v", err)
		}
		if exists {
			t.Error("file should not exist after deletion")
		}
	})

	t.Run("upload with presigned URL", func(t *testing.T) {
		testKey := "presigned-test-file.txt"
		defer func() {
			// Clean up
			client.DeleteFile(ctx, testKey)
		}()

		// Upload with presigned URL return type
		result, err := client.Upload(ctx, testContent, testKey, UploadOptions{
			ReturnType:      S3ReturnTypePresignedUrl,
			PresignedUrlTTL: 10 * time.Minute,
		})
		if err != nil {
			t.Fatalf("failed to upload file with presigned URL: %v", err)
		}

		if !strings.HasPrefix(result, "https://") {
			t.Errorf("expected presigned URL to start with https://, got %s", result)
		}

		// Verify file was uploaded
		exists, err := client.FileExists(ctx, testKey)
		if err != nil {
			t.Errorf("failed to check if file exists: %v", err)
		}
		if !exists {
			t.Error("file should exist after upload")
		}
	})

	t.Run("upload with metadata", func(t *testing.T) {
		testKey := "metadata-test-file.txt"
		defer func() {
			// Clean up
			client.DeleteFile(ctx, testKey)
		}()

		metadata := map[string]string{
			"original-name": "original-file.txt",
			"user-id":       "test-user",
		}

		// Upload with metadata
		_, err := client.Upload(ctx, testContent, testKey, UploadOptions{
			ReturnType: S3ReturnTypeKey,
			Metadata:   metadata,
		})
		if err != nil {
			t.Fatalf("failed to upload file with metadata: %v", err)
		}

		// Get file info to verify metadata
		fileInfo, err := client.GetS3FileInfo(ctx, testBucket, testKey)
		if err != nil {
			t.Errorf("failed to get file info: %v", err)
		} else {
			if fileInfo.Name != metadata["original-name"] {
				t.Errorf("expected original name %s, got %s", metadata["original-name"], fileInfo.Name)
			}
		}
	})

	t.Run("download non-existent file", func(t *testing.T) {
		_, err := client.Download(ctx, "non-existent-file.txt")
		if err == nil {
			t.Error("expected error when downloading non-existent file")
		}
	})

	t.Run("delete non-existent file", func(t *testing.T) {
		// S3 DeleteObject is idempotent - it succeeds even if file doesn't exist
		err := client.DeleteFile(ctx, "non-existent-file.txt")
		if err != nil {
			t.Errorf("DeleteFile should succeed for non-existent file, got error: %v", err)
		}
	})

	t.Run("file exists for non-existent file", func(t *testing.T) {
		exists, err := client.FileExists(ctx, "non-existent-file.txt")
		if err != nil {
			t.Errorf("FileExists should not return error for non-existent file: %v", err)
		}
		if exists {
			t.Error("non-existent file should not exist")
		}
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("empty file upload and download", func(t *testing.T) {
		client, err := New(S3Config{
			Bucket: testBucket,
			Region: testRegion,
		})
		if err != nil {
			t.Fatalf("failed to create S3 client: %v", err)
		}

		ctx := context.Background()
		testKey := "empty-file-test.txt"
		emptyContent := []byte("")

		defer func() {
			client.DeleteFile(ctx, testKey)
		}()

		// Upload empty file
		_, err = client.Upload(ctx, emptyContent, testKey)
		if err != nil {
			t.Fatalf("failed to upload empty file: %v", err)
		}

		// Download empty file
		downloadedContent, err := client.Download(ctx, testKey)
		if err != nil {
			t.Fatalf("failed to download empty file: %v", err)
		}

		if len(downloadedContent) != 0 {
			t.Errorf("expected empty content, got %d bytes", len(downloadedContent))
		}

		// Get file info for empty file
		fileInfo, err := client.GetS3FileInfo(ctx, testBucket, testKey)
		if err != nil {
			t.Errorf("failed to get empty file info: %v", err)
		} else {
			if fileInfo.Size != 0 {
				t.Errorf("expected file size 0, got %d", fileInfo.Size)
			}
		}
	})

	t.Run("large file handling", func(t *testing.T) {

		// Create a 1MB test file
		largeContent := make([]byte, 1024*1024)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		client, err := New(S3Config{
			Bucket: testBucket,
			Region: testRegion,
		})
		if err != nil {
			t.Fatalf("failed to create S3 client: %v", err)
		}

		ctx := context.Background()
		testKey := "large-file-test.bin"

		defer func() {
			client.DeleteFile(ctx, testKey)
		}()

		// Upload large file
		_, err = client.Upload(ctx, largeContent, testKey, UploadOptions{
			FileSize: int64(len(largeContent)),
		})
		if err != nil {
			t.Fatalf("failed to upload large file: %v", err)
		}

		// Download and verify
		downloadedContent, err := client.Download(ctx, testKey)
		if err != nil {
			t.Fatalf("failed to download large file: %v", err)
		}

		if len(downloadedContent) != len(largeContent) {
			t.Errorf("large file size mismatch: expected %d, got %d", len(largeContent), len(downloadedContent))
		}

		// Verify first and last few bytes
		for i := range 10 {
			if downloadedContent[i] != largeContent[i] {
				t.Errorf("large file content mismatch at byte %d", i)
				break
			}
		}
	})

	t.Run("special characters in keys", func(t *testing.T) {

		client, err := New(S3Config{
			Bucket: testBucket,
			Region: testRegion,
		})
		if err != nil {
			t.Fatalf("failed to create S3 client: %v", err)
		}

		ctx := context.Background()
		specialKey := "special-file !@#$%^&()_+-=.txt"
		testContent := []byte("content with special key")

		defer func() {
			client.DeleteFile(ctx, specialKey)
		}()

		// Upload with special characters in key
		_, err = client.Upload(ctx, testContent, specialKey)
		if err != nil {
			t.Fatalf("failed to upload file with special key: %v", err)
		}

		// Download and verify
		downloadedContent, err := client.Download(ctx, specialKey)
		if err != nil {
			t.Fatalf("failed to download file with special key: %v", err)
		}

		if string(downloadedContent) != string(testContent) {
			t.Error("content mismatch for file with special key")
		}
	})
}
