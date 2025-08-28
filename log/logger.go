package log

import (
	"context"
	"os"

	"github.com/finch-technologies/go-utils/log/logstorage"
	"github.com/finch-technologies/go-utils/log/zero"
	"github.com/rs/zerolog"
)

var hasInit bool = false

type Logger struct {
	Db logstorage.ILogStore
}

func Init() {

	if hasInit {
		return
	}

	db, error := logstorage.GetDatabase()

	var z *zero.ZeroLogger

	if error != nil {
		z = zero.New(context.Background(), nil, nil)
	} else {
		z = zero.New(context.Background(), nil, db)
	}

	zerolog.DefaultContextLogger = z.GetLogger()

	hasInit = true
}

func New(ctx context.Context, ctxFields interface{}) LoggerInterface {
	Init()

	db, _ := logstorage.GetDatabase()

	logdriver := os.Getenv("LOG_DRIVER")

	var logger LoggerInterface

	switch logdriver {
	case "zerolog":
		logger = zero.New(ctx, ctxFields, db)
	default:
		logger = zero.New(ctx, ctxFields, db)
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
