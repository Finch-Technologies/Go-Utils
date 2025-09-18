package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/finch-technologies/go-utils/storage/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
	Client *s3.Client
	Bucket string
}

func GetS3Storage(bucket string, region string) (*S3Storage, error) {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		//errors.ThrowError(err, "", "", nil)
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}

	var client *s3.Client

	if os.Getenv("S3_DEBUG") == "true" {
		client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.ClientLogMode = aws.LogSigning | aws.LogRequest | aws.LogResponseWithBody
		})
	} else {
		client = s3.NewFromConfig(cfg)
	}

	return &S3Storage{
		Client: client,
		Bucket: bucket,
	}, nil
}

func (s *S3Storage) Upload(ctx context.Context, file []byte, key string, options ...types.UploadOptions) (string, error) {
	// Default to presigned URLs
	isPresigned := true

	if len(options) > 0 {
		isPresigned = options[0].Presigned
	}

	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.Bucket,
		Key:    &key,
		Body:   bytes.NewReader(file),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload file to S3: %v", err)
	}

	url := fmt.Sprintf("https://af-south-1.console.aws.amazon.com/s3/buckets/%s/%s", s.Bucket, key)

	if isPresigned {
		presignClient := s3.NewPresignClient(s.Client)
		req, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket:          &s.Bucket,
			Key:             &key,
			ResponseExpires: aws.Time(time.Now().Add(30 * 24 * time.Hour)), //Valid for 30 days
		})
		if err != nil {
			return "", fmt.Errorf("failed to generate presigned URL: %v", err)
		}
		url = req.URL
	}

	return url, nil
}

func (s *S3Storage) Download(ctx context.Context, key, filePath string) error {
	output, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.Bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3, %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			_ = fmt.Errorf("failed to write file %q, %v", filePath, err)
		}
	}(output.Body)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %q, %v", filePath, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			_ = fmt.Errorf("failed to write file %q, %v", filePath, err)
		}
	}(file)

	_, err = io.Copy(file, output.Body)
	if err != nil {
		return fmt.Errorf("failed to write file %q, %v", filePath, err)
	}

	return nil
}
