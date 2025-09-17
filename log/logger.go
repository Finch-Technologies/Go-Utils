package log

import (
	"context"
	"os"

	"github.com/finch-technologies/go-utils/log/zero"
	"github.com/rs/zerolog"
)

var hasInit bool = false

type Logger struct {
}

func Init() {

	if hasInit {
		return
	}

	z := zero.New(context.Background(), nil)

	zerolog.DefaultContextLogger = z.GetLogger()

	hasInit = true
}

func New(ctx context.Context, ctxFields interface{}) LoggerInterface {
	Init()

	logdriver := os.Getenv("LOG_DRIVER")

	var logger LoggerInterface

	switch logdriver {
	case "zerolog":
		logger = zero.New(ctx, ctxFields)
	default:
		logger = zero.New(ctx, ctxFields)
	}

	return logger
}

func FromContext(ctx context.Context) LoggerInterface {
	logdriver := os.Getenv("LOG_DRIVER")
	var logger LoggerInterface
	switch logdriver {
	case "zerolog":
		logger = zero.FromContext(ctx)
	default:
		logger = zero.FromContext(ctx)
	}
	return logger
}
