package redis

import (
	"context"

	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/config/database"

	"github.com/redis/go-redis/v9"
)

type RedisLogDriver struct {
	rdb *redis.Client
	ctx context.Context
}

func New(db database.Name) *RedisLogDriver {
	return &RedisLogDriver{
		rdb: adapters.GetRedisClient(db),
		ctx: context.Background(),
	}
}

// This should be PushToList, but has to match the function signature for the io.Writer interface
func (r *RedisLogDriver) Write(p []byte) (n int, err error) {
	err = r.rdb.LPush(context.Background(), "events", string(p)).Err()
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (r *RedisLogDriver) FetchListBatch(listName string, count int64) ([]string, error) {
	eventsJSON, err := r.rdb.LRange(context.Background(), listName, 0, count-1).Result()
	if err != nil {
		return nil, err
	}
	return eventsJSON, nil
}

func (r *RedisLogDriver) DeleteListBatch(listName string, count int64) error {
	err := r.rdb.LTrim(context.Background(), listName, count, -1).Err()
	if err != nil {
		return err
	}
	return nil
}
