package database

import (
	"errors"
	"os"
	"time"

	"github.com/finch-technologies/go-utils/database/dynamodb"
	"github.com/finch-technologies/go-utils/database/redis"
	"github.com/finch-technologies/go-utils/database/types"
	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
)

type IDatabase interface {
	GetString(key string) (string, error)
	GetListWithPrefix(prefix string, limit int64) ([]string, error)
	Set(key string, value any, expiration time.Duration)
	SetWithSortKey(pk string, sk string, value any, expiration time.Duration)
	Get(key string) ([]byte, error)
	Delete(key string) error
}

var dbMap map[string]IDatabase = make(map[string]IDatabase)

func Init(options ...types.DbOptions) (IDatabase, error) {
	log.Debug("Initializing database...")

	if len(options) == 0 || options[0].DbName == "" {
		return nil, errors.New("db name is required")
	}

	dbDriver := utils.StringOrDefault(os.Getenv("DATABASE_DRIVER"), "redis")

	if options[0].Driver != "" {
		dbDriver = options[0].Driver
	}

	dbName := options[0].DbName

	var err error
	db := dbMap[dbName]

	if db == nil {
		switch dbDriver {
		case "redis":
			db, err = redis.New(options...)
			if err != nil {
				return nil, err
			}
		case "dynamodb":
			db, err = dynamodb.New(options...)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("invalid database driver")
		}
		dbMap[dbName] = db
	}

	return db, nil
}

func GetDatabase(dbName string, driver ...string) (IDatabase, error) {

	dbDriver := utils.StringOrDefault(os.Getenv("DATABASE_DRIVER"), "redis")

	if len(driver) > 0 {
		dbDriver = driver[0]
	}

	db := dbMap[dbName]

	if db == nil {
		return Init(types.DbOptions{Driver: dbDriver, DbName: dbName})
	}

	return db, nil
}
