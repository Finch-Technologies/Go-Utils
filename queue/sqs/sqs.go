package sqs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/finch-technologies/go-utils/queue/types"
)

// SQSMessageQueue is a concrete implementation of IMessageQueue using AWS SQS.
type SQSMessageQueue struct {
	client *sqs.Client
	config SQSConfig
}

// New initializes a new SQSQueue instance.
func New(cfg SQSConfig) (*SQSMessageQueue, error) {

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	client := sqs.NewFromConfig(awsCfg)
	return &SQSMessageQueue{client: client, config: cfg}, nil
}

// getQueueURL retrieves the URL of the SQS queue by name.
func (q *SQSMessageQueue) getQueueURL(queueName string) string {
	baseUrl := q.config.SQSBaseUrl
	return fmt.Sprintf("%s/%s", baseUrl, queueName)
}

// Count returns the number of messages in the specified queue.
func (q *SQSMessageQueue) Count(ctx context.Context, queueName string) (int, error) {
	url := q.getQueueURL(queueName)
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
	url := q.getQueueURL(queueName)

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
			BatchSize:       1,
			DeleteMessage:   true,
			ParseFunc:       nil,
		}
	}
	return options[0]
}

// Dequeue receives a message from the specified queue and deletes it after processing.
func (q *SQSMessageQueue) Dequeue(ctx context.Context, queueName string, options ...types.DequeueOptions) ([]types.DequeuedMessage, error) {
	opts := getDequeueOptions(options)
	url := q.getQueueURL(queueName)

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(url),
		MaxNumberOfMessages: int32(opts.BatchSize),
		WaitTimeSeconds:     int32(opts.WaitTimeSeconds),
		MessageAttributeNames: []string{
			string(sqstypes.QueueAttributeNameAll),
		},
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameAll,
		},
	}

	// CRITICAL: Use background context for AWS call to prevent message loss during shutdown.
	// If the parent context is cancelled mid-request, AWS may have already dequeued messages
	// but they would be lost until visibility timeout expires. Let the AWS call complete.
	resp, err := q.client.ReceiveMessage(context.Background(), input)

	if err != nil {
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}

	if len(resp.Messages) == 0 {
		return nil, nil
	}

	messages := make([]types.DequeuedMessage, len(resp.Messages))
	for i, message := range resp.Messages {
		// Extract ApproximateReceiveCount from message attributes
		approximateReceiveCount := 0
		if message.Attributes != nil {
			if countStr, ok := message.Attributes["ApproximateReceiveCount"]; ok {
				if count, err := strconv.Atoi(countStr); err == nil {
					approximateReceiveCount = count
				}
			}
		}

		messages[i] = types.DequeuedMessage{
			MessageId:               *message.MessageId,
			ReceiptHandle:           *message.ReceiptHandle,
			Body:                    *message.Body,
			ReceivedAt:              time.Now(),
			ApproximateReceiveCount: approximateReceiveCount,
		}
	}

	if opts.DeleteMessage {
		for _, message := range resp.Messages {
			_, err = q.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(url),
				ReceiptHandle: message.ReceiptHandle,
			})

			if err != nil {
				return nil, fmt.Errorf("failed to delete message: %w", err)
			}
		}
	}

	return messages, nil
}

// Delete deletes a message from the specified queue.
func (q *SQSMessageQueue) Delete(ctx context.Context, queueName string, id string) error {
	url := q.getQueueURL(queueName)
	_, err := q.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(url),
		ReceiptHandle: aws.String(id),
	})
	return err
}
