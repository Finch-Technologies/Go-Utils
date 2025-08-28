package database

import (
	"errors"
	"os"
	"time"

	"github.com/finch-technologies/go-utils/config/database"
	"github.com/finch-technologies/go-utils/database/dynamodb"
	"github.com/finch-technologies/go-utils/database/redis"
	"github.com/finch-technologies/go-utils/log"
)

type IDatabase interface {
	GetString(key string) (string, error)
	GetListWithPrefix(prefix string, limit int64) ([]string, error)
	Set(key string, value any, expiration time.Duration)
	SetWithSortKey(pk string, sk string, value any, expiration time.Duration)
	Get(key string) ([]byte, error)
	Delete(key string) error
}

var dbMap map[database.Name]IDatabase = make(map[database.Name]IDatabase)

func Init(dbName database.Name) (IDatabase, error) {
	log.Debug("Initializing database....")

	db := dbMap[dbName]

	if db == nil {
		switch os.Getenv("DATABASE_DRIVER") {
		case "redis":
			db = redis.New(dbName)
		case "dynamodb":
			db = dynamodb.New(dbName)
		default:
			return nil, errors.New("invalid database driver")
		}
		dbMap[dbName] = db
	}

	return db, nil
}

func GetDatabase(dbName database.Name) (IDatabase, error) {

	db := dbMap[dbName]

	if db == nil {
		return Init(dbName)
	}

	return db, nil
}

func GetMainDatabase() (IDatabase, error) {
	return GetDatabase(database.Name("main"))
}
