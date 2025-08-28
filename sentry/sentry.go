package sentry

import (
	"os"
	"strconv"

	"github.com/finch-technologies/go-utils/log"
	"github.com/getsentry/sentry-go"
)

// InitSentry initializes Sentry with the given DSN and environment
func Init() {
	dsn := os.Getenv("SENTRY_DSN")
	sampleRate, _ := strconv.ParseFloat(os.Getenv("SENTRY_SAMPLE_RATE"), 64)
	stage := os.Getenv("APP_ENV")
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      stage,
		TracesSampleRate: sampleRate,
	})
	if err != nil {
		log.Error(err)
	}
}
