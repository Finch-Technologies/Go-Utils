package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client struct {
	Client    *s3.Client
	Bucket    string
	KeyPrefix string
	Region    string
}

type S3Config struct {
	Bucket    string
	Region    string
	KeyPrefix string
}

func New(config ...S3Config) (*S3Client, error) {
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

	return &S3Client{
		Client:    client,
		Bucket:    cfg.Bucket,
		KeyPrefix: cfg.KeyPrefix,
		Region:    cfg.Region,
	}, nil
}

func (s *S3Client) Upload(ctx context.Context, file []byte, key string, options ...UploadOptions) (string, error) {
	opts := getUploadOptions(options...)

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

	_, err := s.Client.PutObject(ctx, putObjectInput)

	if err != nil {
		return "", fmt.Errorf("failed to upload file to S3: %v", err)
	}

	if s.KeyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
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

// GeneratePresignedURL generates a presigned URL for file access
func (s *S3Client) GeneratePresignedURL(ctx context.Context, key string, expirationMinutes int) (string, error) {
	presignClient := s3.NewPresignClient(s.Client)

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

func (s *S3Client) Download(ctx context.Context, key string) ([]byte, error) {
	output, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
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
func (s *S3Client) DeleteFile(ctx context.Context, key string) error {
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete file from S3: %w", err)
	}

	return nil
}

// FileExists checks if a file exists in S3
func (s *S3Client) FileExists(ctx context.Context, key string) (bool, error) {
	_, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
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
func (s *S3Client) GetS3FileInfo(ctx context.Context, bucket, key string) (*FileInfo, error) {
	result, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
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
