package redis

import (
	"context"
	"fmt"

	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/config/database"
	"github.com/finch-technologies/go-utils/queues/types"

	"github.com/redis/go-redis/v9"
)

type RedisMessageQueue struct {
	rdb *redis.Client
}

func New(db database.Name) *RedisMessageQueue {
	return &RedisMessageQueue{
		rdb: adapters.GetRedisClient(db),
	}
}

func (msgQueue *RedisMessageQueue) Count(ctx context.Context, queue string) (int, error) {
	count := msgQueue.rdb.LLen(ctx, queue).Val()
	return int(count), nil
}

func (msgQueue *RedisMessageQueue) Enqueue(ctx context.Context, queue string, payload string, options ...types.EnqueueOptions) error {
	err := msgQueue.rdb.LPush(ctx, queue, payload).Err()
	if err != nil {
		return fmt.Errorf("failed to push to the queue: %s", err)
	}
	return nil
}

func (msgQueue *RedisMessageQueue) Dequeue(ctx context.Context, queue string, options ...types.DequeueOptions) ([]string, error) {
	// TODO: Implement batch dequeue
	items := []string{}

	for i := 0; i < options[0].BatchSize; i++ {
		item, err := msgQueue.rdb.RPop(ctx, queue).Result()

		if err == nil {
			items = append(items, item)
		} else if err == redis.Nil {
			break
		} else {
			return nil, fmt.Errorf("failed to get item from queue: %s", err)
		}
	}

	return items, nil
}

func (msgQueue *RedisMessageQueue) Delete(ctx context.Context, queue string, message string) error {
	// Redis does not support deleting a specific message from a queue since dequeue always removes the last item
	return nil
}
