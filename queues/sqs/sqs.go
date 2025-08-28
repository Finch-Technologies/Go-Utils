package sqs

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/finch-technologies/go-utils/queues/types"
)

// SQSMessageQueue is a concrete implementation of IMessageQueue using AWS SQS.
type SQSMessageQueue struct {
	client *sqs.Client
}

// New initializes a new SQSQueue instance.
func New() (*SQSMessageQueue, error) {

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := sqs.NewFromConfig(cfg)
	return &SQSMessageQueue{client: client}, nil
}

// getQueueURL retrieves the URL of the SQS queue by name.
func getQueueURL(queueName string) string {
	baseUrl := os.Getenv("AWS_SQS_BASE_URL")
	return fmt.Sprintf("%s/%s", baseUrl, queueName)
}

// Count returns the number of messages in the specified queue.
func (q *SQSMessageQueue) Count(ctx context.Context, queueName string) (int, error) {
	url := getQueueURL(queueName)
	resp, err := q.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
		},
	})

	if err != nil {
		return 0, fmt.Errorf("failed to get queue attributes: %w", err)
	}

	countStr := resp.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)]
	if countStr == "" {
		return 0, errors.New("queue attribute not found")
	}

	var count int
	fmt.Sscanf(countStr, "%d", &count)
	return count, nil
}

// Enqueue sends a message to the specified queue.
func (q *SQSMessageQueue) Enqueue(ctx context.Context, queueName string, payload string, options ...types.EnqueueOptions) error {
	url := getQueueURL(queueName)

	opts := getEnqueueOptions(options)

	sqsInput := &sqs.SendMessageInput{
		QueueUrl:       aws.String(url),
		MessageBody:    aws.String(payload),
		MessageGroupId: aws.String(opts.MessageGroupId),
	}

	if opts.DeduplicationId != "" {
		sqsInput.MessageDeduplicationId = aws.String(opts.DeduplicationId)
	}

	_, err := q.client.SendMessage(ctx, sqsInput)

	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}
	return nil
}

func getEnqueueOptions(options []types.EnqueueOptions) types.EnqueueOptions {
	if len(options) == 0 {
		return types.EnqueueOptions{
			MessageGroupId: "default",
		}
	}
	return options[0]
}

func getDequeueOptions(options []types.DequeueOptions) types.DequeueOptions {
	if len(options) == 0 {
		return types.DequeueOptions{
			WaitTimeSeconds: 10,
		}
	}
	return options[0]
}

// Dequeue receives a message from the specified queue and deletes it after processing.
func (q *SQSMessageQueue) Dequeue(ctx context.Context, queueName string, options ...types.DequeueOptions) (string, error) {
	opts := getDequeueOptions(options)
	url := getQueueURL(queueName)
	resp, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(url),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     int32(opts.WaitTimeSeconds),
	})

	if err != nil {
		return "", fmt.Errorf("failed to receive message: %w", err)
	}

	if len(resp.Messages) == 0 {
		return "", nil
	}

	message := resp.Messages[0]

	_, err = q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		return "", fmt.Errorf("failed to delete message: %w", err)
	}

	return *message.Body, nil
}
