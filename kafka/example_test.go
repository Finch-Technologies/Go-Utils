package kafka_test

import (
	"context"
	"errors"

	"github.com/finch-technologies/go-utils/kafka"
	"github.com/finch-technologies/go-utils/kafka/contracts"
)

// A service defines its own topics, message types and payloads.
const (
	topicOrderEvents     = "orders.order-events"
	typeOrderPlaced      = "orders.order.placed"
	orderPlacedVersion   = 1
	topicOrderEventsDLQ  = "billing.order-events.dlq"
	billingConsumerGroup = "billing.order-events"
)

type OrderPlaced struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// Producing a message: envelope, encode, produce keyed by order ID.
func ExampleClient_Produce() {
	ctx := context.Background()
	cfg, err := kafka.ConfigFromEnv("orders") // KAFKA_BROKERS, KAFKA_AUTH, AWS_REGION, KAFKA_TOPIC_PREFIX
	if err != nil {
		panic(err)
	}
	client, err := kafka.New(ctx, cfg)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	msg := contracts.New(typeOrderPlaced, orderPlacedVersion, "orders", "acme",
		OrderPlaced{OrderID: "0193…", Amount: 1500})
	value, _ := contracts.Encode(msg)
	_ = client.Produce(ctx, topicOrderEvents, msg.Data.OrderID, value, nil)
}

// Consuming: decode, return kafka.Permanent for messages that can never succeed, a plain
// error for failures worth retrying.
func ExampleClient_Consume() {
	ctx := context.Background()
	cfg, _ := kafka.ConfigFromEnv("billing")
	client, _ := kafka.New(ctx, cfg)
	defer client.Close()

	_ = client.Consume(ctx, kafka.ConsumeOptions{
		Group:    billingConsumerGroup,
		Topics:   []string{topicOrderEvents},
		DLQTopic: topicOrderEventsDLQ,
	}, func(ctx context.Context, r *kafka.Record) error {
		h, err := contracts.Peek(r.Value)
		if err != nil {
			return kafka.Permanent(err)
		}
		if h.Type != typeOrderPlaced {
			return nil // not interested
		}
		order, err := contracts.Decode[OrderPlaced](r.Value, h.Type, orderPlacedVersion)
		if errors.Is(err, contracts.ErrMalformed) || errors.Is(err, contracts.ErrUnsupportedVersion) {
			return kafka.Permanent(err)
		}
		_ = order // process; a transient failure → return err to retry
		return nil
	})
}
