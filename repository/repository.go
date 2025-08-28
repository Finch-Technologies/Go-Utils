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

func Get[T any](key string) (T, error) {

	var value T

	db, err := database.GetMainDatabase()

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

func GetString(key string) (string, error) {
	db, err := database.GetMainDatabase()

	if err != nil {
		return "", err
	}

	return db.GetString(key)
}

func GetInt(key string) (int, error) {
	db, err := database.GetMainDatabase()

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

func Set(key string, value any, expiration time.Duration) error {
	db, err := database.GetMainDatabase()

	if err != nil {
		return err
	}

	db.Set(key, value, expiration)

	return nil
}

func Delete(key string) error {
	db, err := database.GetMainDatabase()

	if err != nil {
		return err
	}

	return db.Delete(key)
}
