package kms

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go/aws"
)

var kmsClient *kms.Client

func getKmsClient(ctx context.Context, awsRegion string) (*kms.Client, error) {

	if kmsClient != nil {
		return kmsClient, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion))

	if err != nil {
		return nil, err
	}

	return kms.NewFromConfig(cfg), nil
}

func Encrypt(ctx context.Context, plaintext string) (string, error) {

	kmsKeyId := os.Getenv("KMS_KEY_ID")

	// Create KMS client
	client, err := getKmsClient(ctx, os.Getenv("AWS_REGION"))

	if err != nil {
		return "", fmt.Errorf("failed to create KMS client: %w", err)
	}

	data := []byte(plaintext)

	req := &kms.EncryptInput{
		KeyId:     aws.String(kmsKeyId),
		Plaintext: data,
	}

	resp, err := client.Encrypt(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt data: %w", err)
	}

	return base64.StdEncoding.EncodeToString(resp.CiphertextBlob), nil
}

func Decrypt(ctx context.Context, ciphertext string, awsRegion string) (string, error) {

	if awsRegion == "" {
		awsRegion = os.Getenv("AWS_REGION")
	}

	// Create KMS client
	client, err := getKmsClient(ctx, awsRegion)

	if err != nil {
		return "", fmt.Errorf("failed to create KMS client: %w", err)
	}

	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("failed to base64 decode ciphertext: %w", err)
	}

	// NOTE: KMS Key ID is not required for decryption
	// Since we are using 2 different keys (old scrapers and shrike) to encrypt the credentials
	// rather let KMS decide which key to use
	req := &kms.DecryptInput{
		//KeyId:          aws.String(os.Getenv("KMS_KEY_ID")),
		CiphertextBlob: data,
	}

	resp, err := client.Decrypt(ctx, req)

	if err != nil {
		return "", fmt.Errorf("failed to decrypt data: %w", err)
	}

	//log.Debug("Decoded ", string(resp.Plaintext))

	return string(resp.Plaintext), nil
}
