package channels

import (
	"context"
	"errors"
	"sync"

	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
)

type SafeChannel[T interface{}] struct {
	mu       sync.Mutex
	isClosed bool
	ch       chan T
	ctx      context.Context
}

func New[T interface{}](ctx context.Context, size ...int) *SafeChannel[T] {
	var ch chan T
	sc := &SafeChannel[T]{}
	if len(size) > 0 {
		ch = make(chan T, size[0])
	} else {
		ch = make(chan T)
	}
	sc.init(ctx, ch)
	return sc
}

func (c *SafeChannel[T]) init(ctx context.Context, ch chan T) {
	c.mu = sync.Mutex{}
	c.ctx = ctx
	c.isClosed = false
	c.ch = ch
	go utils.Try(func() {
		// Ensure the channel is closed when the context is done
		<-ctx.Done()
		c.Close()
	}, log.FromContext(ctx))
}

func (c *SafeChannel[T]) Close() {
	c.mu.Lock()
	if !c.isClosed {
		c.isClosed = true
		close(c.ch)
	}
	c.mu.Unlock()
}

func (c *SafeChannel[T]) Write(data T) error {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return errors.New("channel is closed")
	}
	c.mu.Unlock()
	c.ch <- data

	return nil
}

func (c *SafeChannel[T]) Read() <-chan T {
	return c.ch
}
