package pubsub

import (
	"context"

	"github.com/finch-technologies/go-utils/pubsub/redis"
)

type IMessageBroker interface {
	Publish(ctx context.Context, channel string, payload interface{}) error
	Subscribe(ctx context.Context, channel string, callback func(channel string, payload string)) func() error
}

type MessageBrokerOptions struct {
	Db     int
	Driver MessageBrokerDriver
}

type MessageBrokerDriver string

const (
	MessageBrokerDriverRedis MessageBrokerDriver = "redis"
)

var msgBroker IMessageBroker

func getOptions(options ...MessageBrokerOptions) MessageBrokerOptions {
	if len(options) > 0 {
		return options[0]
	}
	return MessageBrokerOptions{
		Db:     3,
		Driver: MessageBrokerDriverRedis,
	}
}

func Init(options ...MessageBrokerOptions) (IMessageBroker, error) {

	opts := getOptions(options...)

	if msgBroker == nil {
		switch opts.Driver {
		case MessageBrokerDriverRedis:
			msgBroker = redis.New(opts.Db) //pubsub db
		default:
			msgBroker = redis.New(opts.Db)
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
