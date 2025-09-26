package zero

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/finch-technologies/go-utils/env"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
)

type ZeroLogger struct {
	logger  *zerolog.Logger
	context context.Context
}

func New(ctx context.Context, ctxFields any) *ZeroLogger {
	godotenv.Load()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if os.Getenv("LOG_LEVEL") == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	var loggerCtx zerolog.Context

	var cw io.Writer = os.Stdout
	if env.IsLocal() {
		cw = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}
	}

	var mw io.Writer = cw

	loggerCtx = zerolog.New(mw).With().Timestamp()

	if ctxFields != nil {
		for key, value := range getKeyValues(ctxFields) {
			if value != "" {
				loggerCtx = loggerCtx.Str(key, value)
			}
		}
	}

	logger := loggerCtx.Logger()

	return &ZeroLogger{
		logger:  &logger,
		context: logger.WithContext(ctx),
	}
}

func FromContext(ctx context.Context) *ZeroLogger {

	if ctx == nil || ctx.Err() != nil {
		return New(context.Background(), nil)
	}

	logger := zerolog.Ctx(ctx)

	return &ZeroLogger{
		logger: logger,
	}
}

func getKeyValues(ctx interface{}) map[string]string {
	kvMap := make(map[string]string)

	t := reflect.TypeOf(ctx)

	if t.Kind() != reflect.Struct {
		return kvMap
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := reflect.ValueOf(ctx).Field(i)
		if value.IsValid() {
			kvMap[field.Name] = fmt.Sprint(value)
		}
	}

	return kvMap
}

func (z *ZeroLogger) GetLogger() *zerolog.Logger {
	return z.logger
}

func (z *ZeroLogger) GetContext() context.Context {
	return z.context
}

func (z *ZeroLogger) Debug(v ...any) {
	z.logger.Debug().Msg(fmt.Sprint(v...))
}

func (z *ZeroLogger) Debugf(s string, v ...any) {
	z.logger.Debug().Msg(fmt.Sprintf(s, v...))
}

func (z *ZeroLogger) Info(v ...any) {
	z.logger.Info().Msg(fmt.Sprint(v...))
}

func (z *ZeroLogger) Infof(s string, v ...any) {
	z.logger.Info().Msg(fmt.Sprintf(s, v...))
}

func (z *ZeroLogger) Warning(v ...any) {
	z.logger.Warn().Msg(fmt.Sprint(v...))
}

func (z *ZeroLogger) Warningf(s string, v ...any) {
	z.logger.Warn().Msg(fmt.Sprintf(s, v...))
}

func (z *ZeroLogger) Error(v ...any) {
	z.logger.Error().Stack().Msg(fmt.Sprint(v...))
}

func (z *ZeroLogger) Errorf(s string, v ...any) {
	z.logger.Error().Stack().Msg(fmt.Sprintf(s, v...))
}

func (z *ZeroLogger) ErrorStack(stack, s string, v ...any) {
	z.logger.Error().Stack().Msg(fmt.Sprintf(s, v...) + "\n\n" + stack)
}

func (z *ZeroLogger) InfoEvent(eventType string, data string) {
	z.logger.Info().Str("event", eventType).Msg(data)
}

func (z *ZeroLogger) ErrorEvent(eventType string, data string) {
	z.logger.Error().Str("event", eventType).Msg(data)
}

func (z *ZeroLogger) ErrorEventWithResources(eventType string, screenshot, text, data string) {
	le := z.logger.Error().Str("event", eventType)
	if screenshot != "" {
		le.Str("screenshotUrl", screenshot)
	}
	if text != "" {
		le.Str("textUrl", text)
	}
	le.Msg(data)
}

func (z *ZeroLogger) InfoFile(fileLocation string, data string) {
	z.logger.Info().Str("file", fileLocation).Msg(data)
}

func (z *ZeroLogger) ErrorFile(fileLocation string, data string) {
	z.logger.Error().Str("file", fileLocation).Msg(data)
}

// DebugFields logs a debug level message with structured fields
func (z *ZeroLogger) DebugFields(msg string, fields map[string]any) {
	event := z.logger.Debug()
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

// InfoFields logs an info level message with structured fields
func (z *ZeroLogger) InfoFields(msg string, fields map[string]interface{}) {
	event := z.logger.Info()
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

// ErrorFields logs an error level message with structured fields
func (z *ZeroLogger) ErrorFields(msg string, fields map[string]interface{}) {
	event := z.logger.Error()
	for k, v := range fields {
		event = event.Interface(k, v)
	}
	event.Msg(msg)
}

// Fatal logs a fatal level message and then calls os.Exit(1).
func (z *ZeroLogger) Fatal(v ...any) {
	z.logger.Fatal().Msg(fmt.Sprint(v...))
}

// Fatalf logs a formatted fatal level message and then calls os.Exit(1).
func (z *ZeroLogger) Fatalf(s string, v ...any) {
	z.logger.Fatal().Msg(fmt.Sprintf(s, v...))
}
