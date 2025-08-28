package logstorage

import (
	"errors"
	"os"

	"github.com/finch-technologies/go-utils/config/database"
	"github.com/finch-technologies/go-utils/log/logstorage/dynamodb"
	"github.com/finch-technologies/go-utils/log/logstorage/redis"
	"github.com/joho/godotenv"
)

type ILogStore interface {
	Write(p []byte) (n int, err error)
	FetchListBatch(listName string, count int64) ([]string, error)
	DeleteListBatch(listName string, count int64) error
}

var db ILogStore

func Init() (ILogStore, error) {
	// must load this here because it this function could run before the main function
	godotenv.Load()

	if db == nil {
		switch os.Getenv("LOG_STORAGE_DRIVER") {
		case "redis":
			db = redis.New(database.Name("logs"))
		case "dynamodb":
			db = dynamodb.New(database.Name("logs"))
		default:
			// do not use log.Fatal here because it will stop the program and this is the logging driver
			return nil, errors.New("invalid database driver")
		}
	}

	return db, nil
}

func GetDatabase() (ILogStore, error) {
	if db == nil {
		return Init()
	}

	return db, nil
}
