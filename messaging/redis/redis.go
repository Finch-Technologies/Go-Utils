package redis

import (
	"context"
	"encoding/json"

	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/log"

	"github.com/redis/go-redis/v9"
)

type RedisMessageBroker struct {
	rdb *redis.Client
}

func New(db int) *RedisMessageBroker {
	return &RedisMessageBroker{
		rdb: adapters.GetRedisClient(db),
	}
}

func (msgBroker *RedisMessageBroker) Publish(ctx context.Context, channel string, payload any) error {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	err = msgBroker.rdb.Publish(ctx, channel, string(bytes)).Err()
	if err != nil {
		return err
	}
	return nil
}

func (msgBroker *RedisMessageBroker) Subscribe(ctx context.Context, channel string, callback func(channel string, payload string)) func() error {
	//Use PSubscribe to subscribe to a pattern that can include *
	pubsub := msgBroker.rdb.PSubscribe(ctx, channel)

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)

	if err != nil {
		log.Error("Failed to subscribe", err)
		return pubsub.Close
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	go listen(ch, callback)

	return pubsub.Close
}

func listen(channel <-chan *redis.Message, callback func(channel string, payload string)) {
	for msg := range channel {
		callback(msg.Pattern, msg.Payload)
	}
}
