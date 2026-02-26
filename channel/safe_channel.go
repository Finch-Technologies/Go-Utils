package channel

import (
	"context"
	"errors"
	"sync"

	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
)

type SafeChannel[T any] struct {
	ch   chan T
	ctx  context.Context
	once sync.Once
	done chan struct{}
}

func New[T any](ctx context.Context, size ...int) *SafeChannel[T] {
	var ch chan T
	if len(size) > 0 {
		ch = make(chan T, size[0])
	} else {
		ch = make(chan T)
	}

	sc := &SafeChannel[T]{
		ch:   ch,
		ctx:  ctx,
		done: make(chan struct{}),
	}

	go utils.Try(func() {
		<-ctx.Done()
		sc.Close()
	}, log.FromContext(ctx))

	return sc
}

func (c *SafeChannel[T]) Close() {
	c.once.Do(func() {
		close(c.done)
		close(c.ch)
	})
}

func (c *SafeChannel[T]) Write(data T) error {
	select {
	case <-c.done:
		return errors.New("channel is closed")
	case <-c.ctx.Done():
		return errors.New("channel is closed")
	case c.ch <- data:
		return nil
	}
}

func (c *SafeChannel[T]) TryWrite(data T) (bool, error) {
	select {
	case <-c.done:
		return false, errors.New("channel is closed")
	case <-c.ctx.Done():
		return false, errors.New("channel is closed")
	case c.ch <- data:
		return true, nil
	default:
		return false, nil
	}
}

func (c *SafeChannel[T]) Read() <-chan T {
	return c.ch
}
