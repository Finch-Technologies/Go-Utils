package env

import (
	"os"
)

type Environment string

const (
	Local Environment = "local"
)

func IsLocal() bool {
	return Get() == Local
}

func Get() Environment {
	return Environment(os.Getenv("ENVIRONMENT"))
}

func GetOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
