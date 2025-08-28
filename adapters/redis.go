package adapters

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/finch-technologies/go-utils/config/database"
	"github.com/redis/go-redis/v9"
)

var redisClientMap map[database.Name]*redis.Client = make(map[database.Name]*redis.Client)

func GetRedisClient(db database.Name) *redis.Client {

	redisClient := redisClientMap[db]

	if redisClient == nil {
		options := &redis.Options{
			Addr:     fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       GetRedisDB(db),
		}

		if os.Getenv("REDIS_SCHEME") == "tls" {
			options.TLSConfig = &tls.Config{}
		}

		redisClient = redis.NewClient(options)
		redisClientMap[db] = redisClient
	}

	return redisClient
}

var redisDbMap map[database.Name]int = map[database.Name]int{
	database.Name("main"):    0,
	database.Name("secrets"): 1,
	database.Name("logs"):    2,
	database.Name("pubsub"):  3,
	database.Name("queue"):   4,
}

func GetRedisDB(dbName database.Name) int {
	return redisDbMap[dbName]
}
