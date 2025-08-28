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
