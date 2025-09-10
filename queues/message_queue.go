package queues

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/finch-technologies/go-utils/config/database"
	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/queues/redis"
	"github.com/finch-technologies/go-utils/queues/sqs"
	"github.com/finch-technologies/go-utils/queues/types"
)

type Queue string

type IMessageQueue interface {
	Count(ctx context.Context, queue string) (int, error)
	Enqueue(ctx context.Context, queue string, payload string, options ...types.EnqueueOptions) error
	Dequeue(ctx context.Context, queue string, options ...types.DequeueOptions) ([]types.DequeuedMessage, error)
	Delete(ctx context.Context, queue string, message string) error
}

var mq IMessageQueue
var err error

func init() {
	switch os.Getenv("QUEUE_DRIVER") {
	case "redis":
		mq = redis.New(database.Name("queue"))
	case "sqs":
		mq, err = sqs.New()
		if err != nil {
			log.Errorf("failed to create sqs message queue: %s\n", err)
		}
	default:
		log.Error("no valid message queue driver specified")
	}
}

func Count(ctx context.Context, queue Queue) (int, error) {

	if mq == nil {
		return 0, fmt.Errorf("no message queue driver found")
	}

	return mq.Count(ctx, string(queue))
}

func Enqueue[T interface{}](ctx context.Context, queue Queue, payload T, options ...types.EnqueueOptions) error {

	if mq == nil {
		return fmt.Errorf("no message queue driver found")
	}

	jsonBytes, err := json.Marshal(payload)

	if err != nil {
		return fmt.Errorf("failed to marshal payload to json: %s", err)
	}

	return mq.Enqueue(ctx, string(queue), string(jsonBytes))
}

func Dequeue[T interface{}](ctx context.Context, queue Queue, options ...types.DequeueOptions) ([]types.QueueMessage[T], error) {

	var messages []types.QueueMessage[T]

	if mq == nil {
		return messages, fmt.Errorf("no message queue driver found")
	}

	dequeuedMessages, err := mq.Dequeue(ctx, string(queue), options...)

	if err != nil {
		return messages, fmt.Errorf("failed to dequeue item from queue: %s", err)
	}

	if len(dequeuedMessages) == 0 {
		return messages, nil
	}

	for _, dequeuedMessage := range dequeuedMessages {
		var payload any

		if options[0].ParseFunc != nil {
			payload, err = options[0].ParseFunc(dequeuedMessage.Body)
		} else {
			err = json.Unmarshal([]byte(dequeuedMessage.Body), &payload)
		}

		if err != nil {
			return messages, fmt.Errorf("failed to unmarshal payload from json: %s", err)
		}

		messages = append(messages, types.QueueMessage[T]{
			MessageId:               dequeuedMessage.MessageId,
			ReceiptHandle:           dequeuedMessage.ReceiptHandle,
			Payload:                 payload.(T),
			ReceivedAt:              dequeuedMessage.ReceivedAt,
			ApproximateReceiveCount: dequeuedMessage.ApproximateReceiveCount,
		})
	}

	return messages, nil
}

func Delete(ctx context.Context, queue Queue, id string) error {

	if mq == nil {
		return fmt.Errorf("no message queue driver found")
	}

	return mq.Delete(ctx, string(queue), id)
}
