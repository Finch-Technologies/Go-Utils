package redis

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

var redisClientMap map[int]*redis.Client = make(map[int]*redis.Client)

func GetRedisClient(db int) *redis.Client {

	redisClient := redisClientMap[db]

	if redisClient == nil {
		options := &redis.Options{
			Addr:     fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       db,
		}

		if os.Getenv("REDIS_SCHEME") == "tls" {
			options.TLSConfig = &tls.Config{}
		}

		redisClient = redis.NewClient(options)
		redisClientMap[db] = redisClient
	}

	return redisClient
}
