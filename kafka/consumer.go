package kafka

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/finch-technologies/go-utils/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Record is a consumed record.
type Record struct {
	Topic     string // full (prefixed) topic name
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
	// Attempt is 1 on first delivery to the handler and counts up on each retry within
	// this consumer. A redelivery after a restart starts at 1 again.
	Attempt int
}

// Handler processes one record. Returning nil marks it done (it will be committed).
// Returning an error retries it with backoff; wrap the error with Permanent to skip the
// retries when retrying can't help (e.g. a malformed message). The context is not
// cancelled when the consumer shuts down, so an in-flight handler can finish.
type Handler func(ctx context.Context, r *Record) error

// Dead-letter record headers, added to the original record's headers.
const (
	HeaderDLQError             = "dlq-error"
	HeaderDLQOriginalTopic     = "dlq-original-topic"
	HeaderDLQOriginalPartition = "dlq-original-partition"
	HeaderDLQOriginalOffset    = "dlq-original-offset"
	HeaderDLQConsumerGroup     = "dlq-consumer-group"
	HeaderDLQAttempts          = "dlq-attempts"
	HeaderDLQFailedAt          = "dlq-failed-at"
)

// ConsumeOptions configures Consume. Group, Topics and DLQTopic are names without the
// environment prefix.
type ConsumeOptions struct {
	Group  string
	Topics []string
	// DLQTopic receives records whose handler kept failing (after Retries) or failed
	// permanently. Required: without it, a failing record could only block its
	// partition or be dropped.
	DLQTopic string
	// Retries is how many times a failing record is retried after its first attempt
	// before it goes to the DLQ (default 5).
	Retries int
	// Backoff is the first retry delay, doubled per retry up to MaxBackoff, with jitter
	// (defaults 1s and 30s).
	Backoff    time.Duration
	MaxBackoff time.Duration
	// MaxConcurrency caps how many partitions are processed in parallel (default: every
	// partition in a poll).
	MaxConcurrency int
	// MaxPollRecords caps the records taken per poll (default 100).
	MaxPollRecords int
	// RebalanceTimeout is how long the group waits for this member during a rebalance.
	// Rebalances wait for the current poll's records to finish, so it must exceed the
	// slowest handler including its retries (default 5m).
	RebalanceTimeout time.Duration
}

func (o ConsumeOptions) withDefaults() ConsumeOptions {
	if o.Retries == 0 {
		o.Retries = 5
	}
	if o.Backoff == 0 {
		o.Backoff = time.Second
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.MaxPollRecords == 0 {
		o.MaxPollRecords = 100
	}
	if o.RebalanceTimeout == 0 {
		o.RebalanceTimeout = 5 * time.Minute
	}
	return o
}

func (o ConsumeOptions) validate() error {
	var errs []error
	if o.Group == "" {
		errs = append(errs, errors.New("Group is required"))
	}
	if len(o.Topics) == 0 {
		errs = append(errs, errors.New("Topics is required"))
	}
	if o.DLQTopic == "" {
		errs = append(errs, errors.New("DLQTopic is required"))
	}
	if o.Retries < 0 || o.MaxConcurrency < 0 || o.MaxPollRecords < 0 {
		errs = append(errs, errors.New("Retries, MaxConcurrency and MaxPollRecords can't be negative"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("kafka: consume options: %w", errors.Join(errs...))
	}
	return nil
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks a handler error as not worth retrying: the record goes straight to
// the dead-letter topic.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// Consume joins the consumer group and runs handler for every record until ctx is
// cancelled. It returns nil after a clean shutdown, or an error if the consumer couldn't
// start.
//
// Each poll's partitions are processed in parallel, each partition's records in order.
// Once they are all done the poll is committed and the group may rebalance; rebalances
// never interrupt a handler. On shutdown, in-flight handlers finish (their context isn't
// cancelled), what finished is committed, and the rest is left for the next consumer.
func (c *Client) Consume(ctx context.Context, opts ConsumeOptions, handler Handler) error {
	if err := opts.validate(); err != nil {
		return err
	}
	o := opts.withDefaults()

	topics := make([]string, len(o.Topics))
	for i, t := range o.Topics {
		topics[i] = c.Topic(t)
	}
	group := c.Group(o.Group)

	cl, err := kgo.NewClient(append(c.base,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.RebalanceTimeout(o.RebalanceTimeout),
		// A new group starts from the earliest retained record, not just new ones.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)...)
	if err != nil {
		return fmt.Errorf("kafka: create consumer for %s: %w", group, err)
	}
	defer func() {
		// With BlockRebalanceOnPoll, leaving the group (in Close) waits for the last
		// poll to be released.
		cl.AllowRebalance()
		cl.Close()
	}()

	log.InfoFields("Kafka consumer started", map[string]any{"group": group, "topics": topics})
	for {
		fetches := cl.PollRecords(ctx, o.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.WarningFields("Kafka fetch error", map[string]any{
				"group": group, "topic": topic, "partition": partition, "error": err.Error(),
			})
		})

		done := c.processPoll(ctx, cl, group, o, handler, fetches)
		if len(done) > 0 {
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			if err := cl.CommitRecords(commitCtx, done...); err != nil {
				// Not fatal: the records are redelivered and handlers are idempotent.
				log.ErrorFields("Kafka commit failed", map[string]any{"group": group, "error": err.Error()})
			}
			cancel()
		}
		cl.AllowRebalance()
		if ctx.Err() != nil {
			break
		}
	}
	log.InfoFields("Kafka consumer stopped", map[string]any{"group": group})
	return nil
}

// processPoll handles one poll and returns, per partition, the last record that is done
// (to commit).
func (c *Client) processPoll(ctx context.Context, cl *kgo.Client, group string, o ConsumeOptions, handler Handler, fetches kgo.Fetches) []*kgo.Record {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		done []*kgo.Record
		sem  chan struct{}
	)
	if o.MaxConcurrency > 0 {
		sem = make(chan struct{}, o.MaxConcurrency)
	}
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		wg.Add(1)
		go func(records []*kgo.Record) {
			defer wg.Done()
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			var last *kgo.Record
			for _, rec := range records {
				if ctx.Err() != nil || !c.handle(ctx, group, o, handler, rec) {
					break // shutting down: leave the rest uncommitted
				}
				last = rec
			}
			if last != nil {
				mu.Lock()
				done = append(done, last)
				mu.Unlock()
			}
		}(p.Records)
	})
	wg.Wait()
	return done
}

// handle runs the handler with retries, then the DLQ. It returns true when the record is
// done (handled or dead-lettered), false if shutdown interrupted it first.
func (c *Client) handle(ctx context.Context, group string, o ConsumeOptions, handler Handler, rec *kgo.Record) bool {
	r := &Record{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       string(rec.Key),
		Value:     rec.Value,
		Headers:   fromKgoHeaders(rec.Headers),
		Timestamp: rec.Timestamp,
	}
	hctx := extractTrace(context.WithoutCancel(ctx), r.Headers)
	fields := func(extra map[string]any) map[string]any {
		f := map[string]any{"group": group, "topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "key": r.Key}
		for k, v := range extra {
			f[k] = v
		}
		return f
	}

	var err error
	for r.Attempt = 1; ; r.Attempt++ {
		if err = callHandler(hctx, handler, r); err == nil {
			return true
		}
		if IsPermanent(err) || r.Attempt > o.Retries {
			break
		}
		log.WarningFields("Kafka handler failed, retrying", fields(map[string]any{"attempt": r.Attempt, "error": err.Error()}))
		if !sleep(ctx, backoff(o, r.Attempt)) {
			return false
		}
	}

	headers := make(map[string]string, len(r.Headers)+7)
	for k, v := range r.Headers {
		headers[k] = v
	}
	headers[HeaderDLQError] = truncate(err.Error(), 1000)
	headers[HeaderDLQOriginalTopic] = r.Topic
	headers[HeaderDLQOriginalPartition] = strconv.Itoa(int(r.Partition))
	headers[HeaderDLQOriginalOffset] = strconv.FormatInt(r.Offset, 10)
	headers[HeaderDLQConsumerGroup] = group
	headers[HeaderDLQAttempts] = strconv.Itoa(r.Attempt)
	headers[HeaderDLQFailedAt] = time.Now().UTC().Format(time.RFC3339)

	for attempt := 1; ; attempt++ {
		dlqErr := c.Produce(hctx, o.DLQTopic, r.Key, r.Value, headers)
		if dlqErr == nil {
			log.ErrorFields("Kafka record sent to dead-letter topic", fields(map[string]any{
				"dlq_topic": c.Topic(o.DLQTopic), "attempts": r.Attempt, "permanent": IsPermanent(err), "error": err.Error(),
			}))
			return true
		}
		// Can't dead-letter it: keep the record (and its partition) waiting rather than
		// lose it.
		log.ErrorFields("Kafka dead-letter publish failed, retrying", fields(map[string]any{"error": dlqErr.Error()}))
		if !sleep(ctx, backoff(o, attempt)) {
			return false
		}
	}
}

// callHandler runs the handler, turning a panic into an error so it is retried like any
// other failure instead of crashing the consumer.
func callHandler(ctx context.Context, handler Handler, r *Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panic: %v", p)
			log.ErrorFields("Kafka handler panicked", map[string]any{"topic": r.Topic, "offset": r.Offset, "panic": fmt.Sprint(p), "stack": string(debug.Stack())})
		}
	}()
	return handler(ctx, r)
}

func backoff(o ConsumeOptions, attempt int) time.Duration {
	d := o.Backoff << min(attempt-1, 30)
	if d <= 0 || d > o.MaxBackoff {
		d = o.MaxBackoff
	}
	// ±20% jitter, so partitions retrying together don't stay in step.
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// sleep waits for d, returning false if ctx is cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
