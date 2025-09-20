package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/finch-technologies/go-utils/queue/redis"
	"github.com/finch-technologies/go-utils/queue/sqs"
	"github.com/finch-technologies/go-utils/queue/types"
	"github.com/finch-technologies/go-utils/utils"
)

type Queue string

type IMessageQueue interface {
	Count(ctx context.Context, queue string) (int, error)
	Enqueue(ctx context.Context, queue string, payload string, options ...types.EnqueueOptions) error
	Dequeue(ctx context.Context, queue string, options ...types.DequeueOptions) ([]types.DequeuedMessage, error)
	Delete(ctx context.Context, queue string, message string) error
}

type QueueDriver string

const (
	QueueDriverRedis QueueDriver = "redis"
	QueueDriverSQS   QueueDriver = "sqs"
)

type QueueConfig struct {
	Driver  QueueDriver
	RedisDb *int
	Region  string
	BaseUrl string
}

var mq IMessageQueue
var err error

func Init(config ...QueueConfig) error {

	redisDb := 4

	if len(config) == 0 {
		return fmt.Errorf("no queue config provided")
	}

	if config[0].RedisDb != nil {
		redisDb = *config[0].RedisDb
	}

	if config[0].Region == "" {
		config[0].Region = utils.StringOrDefault(os.Getenv("AWS_REGION"), "af-south-1")
	}

	switch config[0].Driver {
	case QueueDriverRedis:
		mq = redis.New(redisDb) //queue db
	case QueueDriverSQS:
		if config[0].BaseUrl == "" {
			return fmt.Errorf("sqs base url is required")
		}
		mq, err = sqs.New(sqs.SQSConfig{
			Region:     config[0].Region,
			SQSBaseUrl: config[0].BaseUrl,
		})
		if err != nil {
			return fmt.Errorf("failed to create sqs queue: %s", err)
		}
	default:
		return fmt.Errorf("no valid queue driver specified")
	}

	return nil
}

func Count(ctx context.Context, queue Queue) (int, error) {

	if mq == nil {
		return 0, fmt.Errorf("no queue driver found")
	}

	return mq.Count(ctx, string(queue))
}

func Enqueue[T interface{}](ctx context.Context, queue Queue, payload T, options ...types.EnqueueOptions) error {

	if mq == nil {
		return fmt.Errorf("no queue driver found")
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
		return messages, fmt.Errorf("no queue driver found")
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
		return fmt.Errorf("no queue driver found")
	}

	return mq.Delete(ctx, string(queue), id)
}
