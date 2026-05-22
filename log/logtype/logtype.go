package logtype

import "context"

// Logger is the shared interface implemented by all logger backends.
// It lives in its own package so both the log and log/zero packages can
// reference it without creating an import cycle.
type Logger interface {
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
	Warningf(s string, v ...any)
	Error(v ...any)
	Errorf(s string, v ...any)
	ErrorStack(stack, s string, v ...any)
	DebugFields(msg string, fields map[string]any)
	InfoFields(msg string, fields map[string]any)
	WarningFields(msg string, fields map[string]any)
	ErrorFields(msg string, fields map[string]any)
	Fatal(v ...any)
	Fatalf(s string, v ...any)
	GetContext() context.Context
	WithFields(fields any) Logger
}
