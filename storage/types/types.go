package types

import "time"

type S3ReturnType string

const (
	S3ReturnTypeUrl          S3ReturnType = "url"
	S3ReturnTypePresignedUrl S3ReturnType = "presigned_url"
	S3ReturnTypeKey          S3ReturnType = "key"
)

type UploadOptions struct {
	ReturnType      S3ReturnType
	ContentType     string
	FileSize        int64
	Metadata        map[string]string
	PresignedUrlTTL time.Duration
}
