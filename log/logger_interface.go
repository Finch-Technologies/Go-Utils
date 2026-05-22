package log

import "github.com/finch-technologies/go-utils/log/logtype"

// LoggerInterface is the logger contract used throughout the application.
// It is an alias for logtype.Logger so both names resolve to the same type.
type LoggerInterface = logtype.Logger
