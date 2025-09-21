package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/finch-technologies/go-utils/storage/types"
	"github.com/finch-technologies/go-utils/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
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

func getConfig(config ...S3Config) (*S3Config, error) {
	defaultConfig := S3Config{
		Region: utils.StringOrDefault(os.Getenv("S3_REGION"), "af-south-1"),
	}

	if len(config) == 0 {
		return nil, errors.New("no config provided")
	}

	cfg := defaultConfig

	if len(config) > 0 {
		cfg = config[0]

		if cfg.Bucket == "" {
			return nil, errors.New("bucket is required")
		}

		utils.MergeObjects(&cfg, defaultConfig)
	}

	return &cfg, nil
}

func New(config ...S3Config) (*S3Storage, error) {
	cfg, err := getConfig(config...)

	if err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.TODO(),
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

	return &S3Storage{
		Client:    client,
		Bucket:    cfg.Bucket,
		KeyPrefix: cfg.KeyPrefix,
		Region:    cfg.Region,
	}, nil
}

func getUploadOptions(options ...types.UploadOptions) types.UploadOptions {
	defaultOptions := types.UploadOptions{
		ReturnType:      types.S3ReturnTypeKey,
		PresignedUrlTTL: 30 * time.Minute,
		Metadata:        map[string]string{},
	}

	opts := defaultOptions

	if len(options) > 0 {
		opts = options[0]
		utils.MergeObjects(&opts, defaultOptions)
	}

	return opts
}

func (s *S3Storage) Upload(ctx context.Context, file []byte, key string, options ...types.UploadOptions) (string, error) {
	opts := getUploadOptions(options...)

	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.Bucket,
		Key:           &key,
		Body:          bytes.NewReader(file),
		Metadata:      opts.Metadata,
		ContentType:   &opts.ContentType,
		ContentLength: &opts.FileSize,
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload file to S3: %v", err)
	}

	if s.KeyPrefix != "" {
		key = fmt.Sprintf("%s/%s", s.KeyPrefix, key)
	}

	var result string

	switch opts.ReturnType {
	case types.S3ReturnTypePresignedUrl:
		result, err = s.GeneratePresignedURL(ctx, key, int(opts.PresignedUrlTTL.Minutes()))
		if err != nil {
			return "", fmt.Errorf("failed to generate presigned URL: %w", err)
		}
	case types.S3ReturnTypeUrl:
		result = fmt.Sprintf("https	://%s.console.aws.amazon.com/s3/buckets/%s/%s", s.Region, s.Bucket, key)
	case types.S3ReturnTypeKey:
		result = key
	}

	return result, nil

}

// GeneratePresignedURL generates a presigned URL for file access
func (s *S3Storage) GeneratePresignedURL(ctx context.Context, s3Key string, expirationMinutes int) (string, error) {
	presignClient := s3.NewPresignClient(s.Client)

	request, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s3Key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(expirationMinutes) * time.Minute
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return request.URL, nil
}

func (s *S3Storage) Download(ctx context.Context, key string) ([]byte, error) {
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
func (s *S3Storage) DeleteFile(ctx context.Context, s3Key string) error {
	_, err := s.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete file from S3: %w", err)
	}

	return nil
}

// FileExists checks if a file exists in S3
func (s *S3Storage) FileExists(ctx context.Context, s3Key string) (bool, error) {
	_, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file exists in S3: %w", err)
	}

	return true, nil
}
