package log

import (
	"context"
)

type LoggerInterface interface {
	Debug(v ...any)
	Debugf(s string, v ...any)
	Info(v ...any)
	Infof(s string, v ...any)
	InfoEvent(eventType string, data string)
	ErrorEvent(eventType string, data string)
	ErrorEventWithResources(eventType string, screenshot, text, data string)
	InfoFile(filePath string, data string)
	ErrorFile(filePath string, data string)
	Warning(v ...any)
	Error(v ...any)
	Errorf(s string, v ...any)
	ErrorStack(stack, s string, v ...any)
	DebugFields(msg string, fields map[string]any)
	InfoFields(msg string, fields map[string]interface{})
	ErrorFields(msg string, fields map[string]interface{})
	Fatal(v ...any)
	Fatalf(s string, v ...any)
	GetContext() context.Context
}
