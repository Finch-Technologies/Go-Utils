package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/finch-technologies/go-utils/adapters"
	"github.com/finch-technologies/go-utils/database/types"
	"github.com/finch-technologies/go-utils/log"

	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	rdb *redis.Client
}

func getOptions(options ...types.DbOptions) types.DbOptions {
	if len(options) > 0 {
		return options[0]
	}
	return types.DbOptions{
		PrimaryKey:       "id",
		TTLAttribute:     "expiration_time",
		SortKeyAttribute: "group_id",
		ValueStoreMode:   types.ValueStoreModeString,
		ValueAttribute:   "value",
	}
}

func New(options ...types.DbOptions) (*RedisDB, error) {

	opts := getOptions(options...)

	//Check if the db name is set and is an int
	if opts.DbName == "" {
		return nil, fmt.Errorf("db name is required")
	}

	//Try to convert the db name to an int
	dbId, err := strconv.Atoi(opts.DbName)
	if err != nil {
		return nil, fmt.Errorf("db name must be an int")
	}

	return &RedisDB{
		rdb: adapters.GetRedisClient(dbId),
	}, nil
}

func (r *RedisDB) GetString(key string) (string, error) {
	val, err := r.rdb.Get(context.Background(), key).Result()

	if err == redis.Nil {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("failed to get value from redis: %s", err)
	}

	return val, nil
}

func (r *RedisDB) Get(key string) ([]byte, error) {
	val, err := r.rdb.Get(context.Background(), key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		log.Error("Failed to get value from Redis: ", err)
		return nil, err
	}

	// Convert the Redis string result directly to []byte
	valueBytes := []byte(val)

	return valueBytes, nil
}

func (r *RedisDB) Set(key string, value any, expiration time.Duration) {

	payload := value

	t := reflect.TypeOf(value).Kind()

	if t == reflect.Struct || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
		bytes, err := json.Marshal(value)
		if err != nil {
			log.Error("Failed to marshal payload: ", err)
			return
		}
		payload = string(bytes)
	}

	err := r.rdb.Set(context.Background(), key, payload, expiration).Err()
	if err != nil {
		log.Error("Failed to write value to redis: ", err)
	}
}

func (r *RedisDB) SetWithSortKey(pk string, sk string, value any, expiration time.Duration) {

	payload := value

	t := reflect.TypeOf(value).Kind()

	if t == reflect.Struct || t == reflect.Interface || t == reflect.Map || t == reflect.Slice || t == reflect.Array {
		bytes, err := json.Marshal(value)
		if err != nil {
			log.Error("Failed to marshal payload: ", err)
			return
		}
		payload = string(bytes)
	}

	err := r.rdb.HSet(context.Background(), pk, sk, payload).Err()
	if err != nil {
		log.Error("Failed to write value to redis: ", err)
	}
}

func (r *RedisDB) Delete(key string) error {
	_, err := r.rdb.Del(context.Background(), key).Result()
	if err != nil {
		return fmt.Errorf("failed to delete key from redis: %s", err)
	}

	return nil
}

func (r *RedisDB) GetListWithPrefix(prefix string, limit int64) ([]string, error) {
	ctx := context.Background()

	keys, err := r.rdb.Keys(ctx, prefix+"*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys from redis: %s", err)
	}

	if int64(len(keys)) > limit {
		keys = keys[:limit]
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get values from redis: %s", err)
	}

	var result []string
	for _, v := range values {
		if vStr, ok := v.(string); ok {
			result = append(result, vStr)
		}
	}

	return result, nil
}
