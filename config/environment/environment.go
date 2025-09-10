package environment

import (
	"os"
)

type Environment string

const (
	Local Environment = "local"
)

func IsLocal() bool {
	return GetEnvironment() == Local
}

func GetEnvironment() Environment {
	return Environment(os.Getenv("ENVIRONMENT"))
}

func GetEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
