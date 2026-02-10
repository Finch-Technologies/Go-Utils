package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/finch-technologies/go-utils/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	s3Client  *s3.Client
	Bucket    string
	KeyPrefix string
	Region    string
}

type Config struct {
	Bucket    string
	Region    string
	KeyPrefix string
}

func New(config ...Config) (*Client, error) {
	cfg, err := getConfig(config...)

	if err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.Region),
	)

	if err != nil {
		//errors.ThrowError(err, "", "", nil)
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}

	var client *s3.Client

	if os.Getenv("S3_DEBUG") == "true" {
		client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.ClientLogMode = aws.LogSigning | aws.LogRequest | aws.LogResponseWithBody
		})
	} else {
		client = s3.NewFromConfig(awsCfg)
	}

	return &Client{
		s3Client:  client,
		Bucket:    cfg.Bucket,
		KeyPrefix: cfg.KeyPrefix,
		Region:    cfg.Region,
	}, nil
}

func (s *Client) Upload(ctx context.Context, file []byte, key string, options ...UploadOptions) (string, error) {
	opts := getUploadOptions(options...)

	// Add prefix to key if configured
	if s.KeyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
	}

	putObjectInput := &s3.PutObjectInput{
		Bucket: &s.Bucket,
		Key:    &key,
		Body:   bytes.NewReader(file),
	}

	if opts.Metadata != nil {
		putObjectInput.Metadata = opts.Metadata
	}

	if opts.ContentType != "" {
		putObjectInput.ContentType = &opts.ContentType
	}

	if opts.FileSize != 0 {
		putObjectInput.ContentLength = &opts.FileSize
	}

	_, err := s.s3Client.PutObject(ctx, putObjectInput)

	if err != nil {
		return "", fmt.Errorf("failed to upload file to S3: %v", err)
	}

	var result string

	switch opts.ReturnType {
	case S3ReturnTypePresignedUrl:
		result, err = s.GeneratePresignedURL(ctx, key, int(opts.PresignedUrlTTL.Minutes()))
		if err != nil {
			return "", fmt.Errorf("failed to generate presigned URL: %w", err)
		}
	case S3ReturnTypeUrl:
		result = fmt.Sprintf("https	://%s.console.aws.amazon.com/s3/buckets/%s/%s", s.Region, s.Bucket, key)
	case S3ReturnTypeKey:
		result = key
	}

	return result, nil
}

// ListFiles lists files in the S3 bucket with the configured prefix
func (s *Client) ListFiles(ctx context.Context, maxKeys int32) ([]FileInfo, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(s.KeyPrefix),
	}
	if maxKeys > 0 {
		input.MaxKeys = &maxKeys
	}

	result, err := s.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list files in S3: %w", err)
	}

	var files []FileInfo
	for _, obj := range result.Contents {
		// Extract original filename from key (remove prefix)
		name := strings.TrimPrefix(*obj.Key, s.KeyPrefix)

		// If prefix was removed and there's a leading slash, remove it
		if name != *obj.Key && strings.HasPrefix(name, "/") {
			name = strings.TrimPrefix(name, "/")
		}

		var size int64
		if obj.Size != nil {
			size = *obj.Size
		}

		files = append(files, FileInfo{
			Name:         name,
			Size:         size,
			LastModified: obj.LastModified,
			S3Key:        *obj.Key,
		})
	}

	return files, nil
}

// GeneratePresignedURL generates a presigned URL for file access
func (s *Client) GeneratePresignedURL(ctx context.Context, key string, expirationMinutes int) (string, error) {
	presignClient := s3.NewPresignClient(s.s3Client)

	request, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(expirationMinutes) * time.Minute
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return request.URL, nil
}

func (s *Client) Download(ctx context.Context, key string) ([]byte, error) {
	// Add prefix to key if configured
	if s.KeyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
	}

	output, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.Bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file from S3, %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			_ = fmt.Errorf("failed to write file %q, %v", key, err)
		}
	}(output.Body)

	return io.ReadAll(output.Body)
}

// DeleteFile deletes a file from S3
func (s *Client) DeleteFile(ctx context.Context, key string) error {
	// Add prefix to key if configured
	if s.KeyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
	}

	_, err := s.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete file from S3: %w", err)
	}

	return nil
}

// FileExists checks if a file exists in S3
func (s *Client) FileExists(ctx context.Context, key string) (bool, error) {
	// Add prefix to key if configured
	if s.KeyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
	}

	_, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return false, nil
		}
		// Check for 404 status code in the error message as well
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file exists in S3: %w", err)
	}

	return true, nil
}

// getFileInfoFromAWS gets file info using AWS SDK (for private URLs)
func (s *Client) GetS3FileInfo(ctx context.Context, bucket, key string) (*FileInfo, error) {
	// Add prefix to key if configured and bucket matches client bucket
	if s.KeyPrefix != "" && bucket == s.Bucket {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
	}

	result, err := s.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get file info from S3: %w", err)
	}

	originalName := key
	if result.Metadata != nil && result.Metadata["original-name"] != "" {
		originalName = result.Metadata["original-name"]
	} else {
		// Extract filename from key
		originalName = filepath.Base(key)
	}

	var size int64
	if result.ContentLength != nil {
		size = *result.ContentLength
	}

	info := &FileInfo{
		Name:         originalName,
		Size:         size,
		ContentType:  aws.ToString(result.ContentType),
		LastModified: result.LastModified,
		S3Key:        key,
	}

	return info, nil
}

// isS3URL checks if a URL is an S3 URL
func IsS3URL(rawURL string) bool {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	// Check for s3:// scheme
	if parsedURL.Scheme == "s3" {
		return true
	}

	// Check for amazonaws.com in host
	if strings.Contains(parsedURL.Host, "amazonaws.com") && strings.Contains(parsedURL.Host, "s3") {
		return true
	}

	return false
}

// ParseS3URL parses an S3 URL and extracts bucket name and key
func ParseS3URL(s3URL string) (bucket, key string, err error) {
	parsedURL, err := url.Parse(s3URL)
	if err != nil {
		return "", "", fmt.Errorf("invalid URL format: %w", err)
	}

	// Handle different S3 URL formats:
	// 1. https://bucket.s3.region.amazonaws.com/key
	// 2. https://s3.region.amazonaws.com/bucket/key
	// 3. s3://bucket/key

	if parsedURL.Scheme == "s3" {
		// s3://bucket/key format
		bucket = parsedURL.Host
		key = strings.TrimPrefix(parsedURL.Path, "/")
	} else if strings.Contains(parsedURL.Host, "amazonaws.com") {
		if strings.HasPrefix(parsedURL.Host, "s3.") || strings.HasPrefix(parsedURL.Host, "s3-") {
			// https://s3.region.amazonaws.com/bucket/key format
			pathParts := strings.SplitN(strings.TrimPrefix(parsedURL.Path, "/"), "/", 2)
			if len(pathParts) < 2 {
				return "", "", fmt.Errorf("invalid S3 URL path format")
			}
			bucket = pathParts[0]
			key = pathParts[1]
		} else {
			// https://bucket.s3.region.amazonaws.com/key format
			hostParts := strings.Split(parsedURL.Host, ".")
			if len(hostParts) < 4 {
				return "", "", fmt.Errorf("invalid S3 URL host format")
			}
			// Find the bucket name by removing .s3.region.amazonaws.com suffix
			s3Index := -1
			for i, part := range hostParts {
				if part == "s3" {
					s3Index = i
					break
				}
			}
			if s3Index == -1 {
				return "", "", fmt.Errorf("invalid S3 URL host format - no s3 component found")
			}
			// Bucket name is everything before .s3.region.amazonaws.com
			bucket = strings.Join(hostParts[:s3Index], ".")
			key = strings.TrimPrefix(parsedURL.Path, "/")
		}
	} else {
		return "", "", fmt.Errorf("URL is not a valid S3 URL")
	}

	if bucket == "" || key == "" {
		return "", "", fmt.Errorf("unable to extract bucket and key from S3 URL")
	}

	return bucket, key, nil
}

// GetFileInfoFromHTTP gets file info using HTTP HEAD request (for public/signed URLs)
func GetFileInfoFromHTTP(fileUrl string) (*FileInfo, error) {

	headSuccess := true

	resp, err := http.Head(fileUrl)
	if err != nil || resp.StatusCode != http.StatusOK {
		headSuccess = false

		//If HEAD fails, try minimal GET request that only loads one byte and headers
		resp, err = minimalGet(fileUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to access URL: %w", err)
		}

		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("URL returned status: %d", resp.StatusCode)
		}
	}

	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = utils.GetContentTypeFromURL(fileUrl)
	}

	fileSize := int64(0)

	if headSuccess {
		fileSize = resp.ContentLength
	} else {
		contentRange := resp.Header.Get("Content-Range")
		if contentRange != "" {
			// bytes 0-0/12345
			var totalSize int64
			fmt.Sscanf(contentRange, "bytes %*d-%*d/%d", &totalSize)
			fileSize = totalSize
		}
	}

	if fileSize <= 0 {
		return nil, fmt.Errorf("unable to determine file size from URL")
	}

	// Extract filename from URL path
	parsedURL, _ := url.Parse(fileUrl)
	fileName := filepath.Base(parsedURL.Path)
	if fileName == "" || fileName == "/" {
		fileName = "document" + utils.GetExtensionFromContentType(contentType)
	}

	_, key, _ := ParseS3URL(fileUrl)

	info := &FileInfo{
		Name:        fileName,
		Size:        fileSize,
		ContentType: contentType,
		S3Key:       key,
	}

	return info, nil
}

func minimalGet(url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		url,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// Request only 1 byte
	req.Header.Set("Range", "bytes=0-0")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	return client.Do(req)
}
