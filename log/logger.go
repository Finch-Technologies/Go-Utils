package log

import (
	"context"

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

	return zero.New(ctx, ctxFields)
}

func FromContext(ctx context.Context) LoggerInterface {
	return zero.FromContext(ctx)
}
