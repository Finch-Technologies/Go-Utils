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
	Dequeue(ctx context.Context, queue string, options ...types.DequeueOptions) ([]string, error)
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

func Dequeue[T interface{}](ctx context.Context, queue Queue, options ...types.DequeueOptions) ([]T, error) {

	var payloads []T

	if mq == nil {
		return payloads, fmt.Errorf("no message queue driver found")
	}

	messages, err := mq.Dequeue(ctx, string(queue))

	if err != nil {
		return payloads, fmt.Errorf("failed to dequeue item from queue: %s", err)
	}

	if len(messages) == 0 {
		return payloads, nil
	}

	for _, message := range messages {
		var payload T
		err = json.Unmarshal([]byte(message), &payload)

		if err != nil {
			return payloads, fmt.Errorf("failed to unmarshal payload from json: %s", err)
		}

		payloads = append(payloads, payload)
	}

	return payloads, nil
}
