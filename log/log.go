package log

import (
	"context"
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

func InfoEvent(eventType string, data string) {
	logger.InfoEvent(eventType, data)
}

func ErrorEvent(eventType string, data string) {
	logger.ErrorEvent(eventType, data)
}

func ErrorEventWithResources(eventType string, screenshot, text, data string) {
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

// DebugFields logs a debug level message with structured fields
func DebugFields(msg string, fields map[string]any) {
	logger.DebugFields(msg, fields)
}

// InfoFields logs an info level message with structured fields
func InfoFields(msg string, fields map[string]interface{}) {
	logger.InfoFields(msg, fields)
}

// ErrorFields logs an error level message with structured fields
func ErrorFields(msg string, fields map[string]interface{}) {
	logger.ErrorFields(msg, fields)
}

func Fatal(v ...any) {
	logger.Fatal(v...)
}

func Fatalf(s string, v ...any) {
	logger.Fatalf(s, v...)
}

func GetContext() context.Context {
	return logger.GetContext()
}
