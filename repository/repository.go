package repository

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/finch-technologies/go-utils/database"
)

func GetKey(id string, suffix string) string {
	return id + ":" + suffix
}

func Get[T any](dbName string, key string, driver ...string) (T, error) {

	var value T

	db, err := database.GetDatabase(dbName, driver...)

	if err != nil {
		return value, err
	}

	valueStr, err := db.GetString(key)

	if err != nil {
		return value, err
	}

	if valueStr == "" {
		return value, errors.New("item not found in database")
	}

	err = json.Unmarshal([]byte(valueStr), &value)

	return value, err
}

func GetString(dbName, key string, driver ...string) (string, error) {
	db, err := database.GetDatabase(dbName, driver...)

	if err != nil {
		return "", err
	}

	return db.GetString(key)
}

func GetInt(dbName, key string, driver ...string) (int, error) {
	db, err := database.GetDatabase(dbName, driver...)

	if err != nil {
		return 0, err
	}

	str, err := db.GetString(key)

	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(str)

	if err != nil {
		return 0, err
	}

	return value, nil
}

func Set(dbName, key string, value any, expiration time.Duration, driver ...string) error {
	db, err := database.GetDatabase(dbName, driver...)

	if err != nil {
		return err
	}

	db.Set(key, value, expiration)

	return nil
}

func Delete(dbName, key string, driver ...string) error {
	db, err := database.GetDatabase(dbName, driver...)

	if err != nil {
		return err
	}

	return db.Delete(key)
}
