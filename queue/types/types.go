package types

import "time"

type EnqueueOptions struct {
	MessageGroupId  string
	DeduplicationId string
	Attributes      map[string]string
}

type DequeueOptions struct {
	WaitTimeSeconds int
	BatchSize       int
	DeleteMessage   bool
}

type GenericDequeueOptions[T any] struct {
	WaitTimeSeconds int
	BatchSize       int
	DeleteMessage   bool
	ParseFunc       func(body string) (T, error)
}

type DequeuedMessage struct {
	MessageId               string
	ReceiptHandle           string
	Body                    string
	ReceivedAt              time.Time
	ApproximateReceiveCount int
}

type QueueMessage[T any] struct {
	MessageId               string
	ReceiptHandle           string
	Payload                 T
	ReceivedAt              time.Time
	ApproximateReceiveCount int
}
