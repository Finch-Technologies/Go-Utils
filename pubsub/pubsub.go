package pubsub

import (
	"context"
	"errors"
	"os"

	"github.com/finch-technologies/go-utils/pubsub/redis"
)

type IMessageBroker interface {
	Publish(ctx context.Context, channel string, payload interface{}) error
	Subscribe(ctx context.Context, channel string, callback func(channel string, payload string)) func() error
}

var msgBroker IMessageBroker

func Init() (IMessageBroker, error) {
	if msgBroker == nil {
		switch os.Getenv("MESSAGE_DRIVER") {
		case "redis":
			msgBroker = redis.New(3) //pubsub db
		default:
			return nil, errors.New("invalid message broker driver")
		}
	}

	return msgBroker, nil
}

func GetBroker() (IMessageBroker, error) {
	if msgBroker == nil {
		return Init()
	}

	return msgBroker, nil
}
