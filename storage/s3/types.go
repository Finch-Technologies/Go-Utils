package s3

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

// FileInfo contains information about a stored file
type FileInfo struct {
	Name         string     `json:"name"`
	Size         int64      `json:"size"`
	ContentType  string     `json:"content_type,omitempty"`
	LastModified *time.Time `json:"last_modified,omitempty"`
	S3Key        string     `json:"s3_key,omitempty"`
}
