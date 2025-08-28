package log

import (
	"context"

	"github.com/finch-technologies/go-utils/events"
)

var logger = New(context.Background(), nil)

func Debug(v ...any) {
	logger.Debug(v...)
}

func Debugf(s string, v ...any) {
	logger.Debugf(s, v...)
}

func Info(v ...any) {
	logger.Info(v...)
}

func Infof(s string, v ...any) {
	logger.Infof(s, v...)
}

func InfoEvent(eventType events.Event, data string) {
	logger.InfoEvent(eventType, data)
}

func ErrorEvent(eventType events.Event, data string) {
	logger.ErrorEvent(eventType, data)
}

func ErrorEventWithResources(eventType events.Event, screenshot, text, data string) {
	logger.ErrorEventWithResources(eventType, screenshot, text, data)
}

func InfoFile(filePath string, data string) {
	logger.InfoFile(filePath, data)
}

func ErrorFile(filePath string, data string) {
	logger.ErrorFile(filePath, data)
}

func Warning(v ...any) {
	logger.Warning(v...)
}

func Error(v ...any) {
	logger.Error(v...)
}

func Errorf(s string, v ...any) {
	logger.Errorf(s, v...)
}

func ErrorStack(stack, s string, v ...any) {
	logger.ErrorStack(stack, s, v...)
}

func GetContext() context.Context {
	return logger.GetContext()
}
