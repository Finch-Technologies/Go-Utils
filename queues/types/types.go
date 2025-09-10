package types

type EnqueueOptions struct {
	MessageGroupId  string
	DeduplicationId string
	Attributes      map[string]string
}

type DequeueOptions struct {
	WaitTimeSeconds int
	BatchSize       int
	DeleteMessage   bool
	ParseFunc       func(message string) (any, error)
}
