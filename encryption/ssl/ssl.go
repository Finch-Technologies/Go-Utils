package ssl

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"os"
)

func Encrypt(plaintext string) (string, error) {

	sslKey := os.Getenv("SSL_SECRET_KEY")
	iv := os.Getenv("SSL_IV")

	var plainTextBlock []byte
	length := len(plaintext)

	if length%aes.BlockSize != 0 {
		extendBlock := aes.BlockSize - (length % aes.BlockSize)
		plainTextBlock = make([]byte, length+extendBlock)
		copy(plainTextBlock[length:], bytes.Repeat([]byte{uint8(extendBlock)}, extendBlock))
	} else {
		plainTextBlock = make([]byte, length)
	}

	copy(plainTextBlock, plaintext)
	block, err := aes.NewCipher([]byte(sslKey))

	if err != nil {
		return "", err
	}

	ciphertext := make([]byte, len(plainTextBlock))
	mode := cipher.NewCBCEncrypter(block, []byte(iv))
	mode.CryptBlocks(ciphertext, plainTextBlock)

	str := base64.StdEncoding.EncodeToString(ciphertext)

	return str, nil
}

type DecryptOptions struct {
	SecretKey string
	IV        string
}

func Decrypt(encrypted string, options ...DecryptOptions) (string, error) {

	secretKey := os.Getenv("SSL_SECRET_KEY")
	iv := os.Getenv("SSL_IV")

	if len(options) > 0 {
		optsSecretKey := options[0].SecretKey
		if optsSecretKey != "" {
			secretKey = optsSecretKey
		}
		optsIv := options[0].IV
		if optsIv != "" {
			iv = optsIv
		}
	}

	if secretKey == "" {
		return "", fmt.Errorf("ssl secret key is not defined")
	}

	if iv == "" {
		return "", fmt.Errorf("ssl IV is not defined")
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)

	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher([]byte(secretKey))

	if err != nil {
		return "", err
	}

	if len(ciphertext)%aes.BlockSize != 0 {
		return "", fmt.Errorf("block size cant be zero")
	}

	mode := cipher.NewCBCDecrypter(block, []byte(iv))
	mode.CryptBlocks(ciphertext, ciphertext)
	ciphertext = PKCS5UnPadding(ciphertext)

	return string(ciphertext), nil
}

// PKCS5UnPadding  pads a certain blob of data with necessary data to be used in AES block cipher
func PKCS5UnPadding(src []byte) []byte {
	length := len(src)
	unpadding := int(src[length-1])

	return src[:(length - unpadding)]
}
