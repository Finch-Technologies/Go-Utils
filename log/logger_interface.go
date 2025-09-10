package log

import (
	"context"

	"github.com/finch-technologies/go-utils/events"
)

type LoggerInterface interface {
	Debug(v ...any)
	Debugf(s string, v ...any)
	Info(v ...any)
	Infof(s string, v ...any)
	InfoEvent(eventType events.Event, data string)
	ErrorEvent(eventType events.Event, data string)
	ErrorEventWithResources(eventType events.Event, screenshot, text, data string)
	InfoFile(filePath string, data string)
	ErrorFile(filePath string, data string)
	Warning(v ...any)
	Error(v ...any)
	Errorf(s string, v ...any)
	ErrorStack(stack, s string, v ...any)
	DebugFields(msg string, fields map[string]any)
	InfoFields(msg string, fields map[string]interface{})
	ErrorFields(msg string, fields map[string]interface{})
	GetContext() context.Context
}
