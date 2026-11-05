package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const prefix = "test"

// newTestClient starts an in-memory cluster with the given topics (names without the
// prefix, 3 partitions each) and returns a client for it.
func newTestClient(t *testing.T, topics ...string) *Client {
	t.Helper()
	full := make([]string, len(topics))
	for i, tp := range topics {
		full[i] = prefix + "." + tp
	}
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, full...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	c, err := New(context.Background(), Config{
		Brokers: cluster.ListenAddrs(), Auth: AuthNone, TopicPrefix: prefix, ClientID: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// consumer runs Consume in the background; stop cancels it and waits for it to return.
type consumer struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func startConsumer(t *testing.T, c *Client, opts ConsumeOptions, h Handler) *consumer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cons := &consumer{cancel: cancel, done: make(chan error, 1)}
	go func() { cons.done <- c.Consume(ctx, opts, h) }()
	t.Cleanup(cons.stop)
	return cons
}

func (c *consumer) stop() {
	c.once.Do(func() {
		c.cancel()
		select {
		case <-c.done:
		case <-time.After(15 * time.Second):
			panic("consumer did not stop")
		}
	})
}

// collector is a handler that records what it saw.
type collector struct {
	mu   sync.Mutex
	recs []Record
}

func (c *collector) handle(_ context.Context, r *Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, *r)
	return nil
}

func (c *collector) values() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.recs))
	for i, r := range c.recs {
		out[i] = string(r.Value)
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func opts(group string) ConsumeOptions {
	return ConsumeOptions{
		Group: group, Topics: []string{"events"}, DLQTopic: "events.dlq",
		Retries: 2, Backoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
	}
}

func produce(t *testing.T, c *Client, key string, values ...string) {
	t.Helper()
	for _, v := range values {
		if err := c.Produce(context.Background(), "events", key, []byte(v), map[string]string{"h": "v-" + v}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	ok := Config{Brokers: []string{"b:9098"}, Auth: AuthIAM, Region: "af-south-1", TopicPrefix: "dev", ClientID: "orders"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"no brokers":       func(c *Config) { c.Brokers = nil },
		"unknown auth":     func(c *Config) { c.Auth = "scram" },
		"iam no region":    func(c *Config) { c.Region = "" },
		"no prefix":        func(c *Config) { c.TopicPrefix = "" },
		"prefix with dot":  func(c *Config) { c.TopicPrefix = "dev.x" },
		"uppercase prefix": func(c *Config) { c.TopicPrefix = "Dev" },
		"no client id":     func(c *Config) { c.ClientID = "" },
	} {
		c := ok
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", " b-1:9098 , b-2:9098,")
	t.Setenv("KAFKA_AUTH", "")
	t.Setenv("AWS_REGION", "af-south-1")
	t.Setenv("KAFKA_TOPIC_PREFIX", "uat")
	cfg, err := ConfigFromEnv("billing")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Brokers) != 2 || cfg.Brokers[1] != "b-2:9098" || cfg.Auth != AuthIAM || cfg.TopicPrefix != "uat" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConsumeOptionsValidate(t *testing.T) {
	c := newTestClient(t, "events")
	err := c.Consume(context.Background(), ConsumeOptions{Group: "g", Topics: []string{"events"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "DLQTopic") {
		t.Fatalf("missing DLQ accepted: %v", err)
	}
}

func TestPrefixes(t *testing.T) {
	c := &Client{cfg: Config{TopicPrefix: "dev"}}
	if got := c.Topic("orders.order-events"); got != "dev.orders.order-events" {
		t.Errorf("Topic = %s", got)
	}
	if got := c.Group("billing.order-events"); got != "dev.billing.order-events" {
		t.Errorf("Group = %s", got)
	}
}

func TestRoundTripAndCommit(t *testing.T) {
	c := newTestClient(t, "events", "events.dlq")
	produce(t, c, "k1", "a", "b", "c")

	col := &collector{}
	cons := startConsumer(t, c, opts("g"), col.handle)
	waitFor(t, "3 records", func() bool { return len(col.values()) == 3 })
	cons.stop()

	r := col.recs[0]
	if r.Topic != "test.events" || r.Key != "k1" || string(r.Value) != "a" || r.Headers["h"] != "v-a" || r.Attempt != 1 {
		t.Fatalf("unexpected record: %+v", r)
	}

	// Same group again: the three were committed, so only the new record arrives.
	produce(t, c, "k1", "d")
	col2 := &collector{}
	startConsumer(t, c, opts("g"), col2.handle)
	waitFor(t, "record d", func() bool { return len(col2.values()) >= 1 })
	time.Sleep(200 * time.Millisecond)
	if got := col2.values(); len(got) != 1 || got[0] != "d" {
		t.Fatalf("after restart got %v, want [d]", got)
	}
}

func TestOrderWithinPartition(t *testing.T) {
	c := newTestClient(t, "events", "events.dlq")
	var want []string
	for i := range 40 {
		want = append(want, fmt.Sprint(i))
	}
	produce(t, c, "same-key", want...)

	col := &collector{}
	startConsumer(t, c, opts("g"), func(ctx context.Context, r *Record) error {
		if r.Offset%7 == 0 {
			time.Sleep(5 * time.Millisecond) // uneven handler times
		}
		return col.handle(ctx, r)
	})
	waitFor(t, "40 records", func() bool { return len(col.values()) == 40 })
	if got := strings.Join(col.values(), ","); got != strings.Join(want, ",") {
		t.Fatalf("order not kept:\n got %s\nwant %s", got, strings.Join(want, ","))
	}
}

func TestRetryThenSuccess(t *testing.T) {
	c := newTestClient(t, "events", "events.dlq")
	produce(t, c, "k", "flaky")

	var mu sync.Mutex
	var attempts []int
	startConsumer(t, c, opts("g"), func(_ context.Context, r *Record) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, r.Attempt)
		if r.Attempt < 3 {
			return errors.New("transient")
		}
		return nil
	})
	waitFor(t, "3 attempts", func() bool { mu.Lock(); defer mu.Unlock(); return len(attempts) == 3 })
	time.Sleep(100 * time.Millisecond)

	dlq := &collector{}
	startConsumer(t, c, ConsumeOptions{Group: "dlq-reader", Topics: []string{"events.dlq"}, DLQTopic: "events.dlq.dlq"}, dlq.handle)
	time.Sleep(500 * time.Millisecond)
	if n := len(dlq.values()); n != 0 {
		t.Fatalf("succeeded record was dead-lettered (%d)", n)
	}
	if fmt.Sprint(attempts) != "[1 2 3]" {
		t.Fatalf("attempts = %v", attempts)
	}
}

func TestDeadLetterAfterRetriesAndPermanent(t *testing.T) {
	c := newTestClient(t, "events", "events.dlq")
	produce(t, c, "k", "always-fails", "permanent", "fine")

	var mu sync.Mutex
	attempts := map[string]int{}
	startConsumer(t, c, opts("g"), func(_ context.Context, r *Record) error {
		mu.Lock()
		attempts[string(r.Value)]++
		mu.Unlock()
		switch string(r.Value) {
		case "always-fails":
			return errors.New("still broken")
		case "permanent":
			return Permanent(errors.New("malformed"))
		}
		return nil
	})

	dlq := &collector{}
	startConsumer(t, c, ConsumeOptions{Group: "dlq-reader", Topics: []string{"events.dlq"}, DLQTopic: "events.dlq.dlq"}, dlq.handle)
	waitFor(t, "2 dead letters", func() bool { return len(dlq.values()) == 2 })

	mu.Lock()
	if attempts["always-fails"] != 3 || attempts["permanent"] != 1 || attempts["fine"] != 1 {
		t.Fatalf("attempts = %v (want always-fails 3 = 1 + 2 retries, permanent 1, fine 1)", attempts)
	}
	mu.Unlock()

	byValue := map[string]Record{}
	for _, r := range dlq.recs {
		byValue[string(r.Value)] = r
	}
	r := byValue["always-fails"]
	if r.Topic != "test.events.dlq" || r.Key != "k" || r.Headers["h"] != "v-always-fails" ||
		r.Headers[HeaderDLQError] != "still broken" || r.Headers[HeaderDLQOriginalTopic] != "test.events" ||
		r.Headers[HeaderDLQConsumerGroup] != "test.g" || r.Headers[HeaderDLQAttempts] != "3" {
		t.Fatalf("unexpected dead letter: %+v", r)
	}
	if byValue["permanent"].Headers[HeaderDLQAttempts] != "1" {
		t.Fatalf("permanent error was retried: %+v", byValue["permanent"].Headers)
	}
}

func TestPanicIsRetried(t *testing.T) {
	c := newTestClient(t, "events", "events.dlq")
	produce(t, c, "k", "boom")

	var mu sync.Mutex
	calls := 0
	startConsumer(t, c, opts("g"), func(context.Context, *Record) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			panic("handler bug")
		}
		return nil
	})
	waitFor(t, "retry after panic", func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
}

// On shutdown the in-flight handler finishes and is committed; later records in the same
// poll are left for the next consumer.
func TestShutdownCommitsOnlyFinishedWork(t *testing.T) {
	c := newTestClient(t, "events", "events.dlq")
	produce(t, c, "same-key", "first", "second", "third")

	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var handled []string
	cons := startConsumer(t, c, opts("g"), func(ctx context.Context, r *Record) error {
		if string(r.Value) == "first" {
			close(started)
			<-release
			if ctx.Err() != nil {
				t.Error("handler context was cancelled on shutdown")
			}
		}
		mu.Lock()
		handled = append(handled, string(r.Value))
		mu.Unlock()
		return nil
	})

	<-started
	stopped := make(chan struct{})
	go func() { cons.stop(); close(stopped) }()
	time.Sleep(100 * time.Millisecond) // shutdown requested while "first" is in flight
	close(release)
	<-stopped

	mu.Lock()
	if fmt.Sprint(handled) != "[first]" {
		t.Fatalf("handled %v during shutdown, want [first]", handled)
	}
	mu.Unlock()

	col := &collector{}
	startConsumer(t, c, opts("g"), col.handle)
	waitFor(t, "remaining records", func() bool { return len(col.values()) == 2 })
	if got := fmt.Sprint(col.values()); got != "[second third]" {
		t.Fatalf("next consumer got %s, want [second third]", got)
	}
}

func TestTraceContextPropagates(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator()) })

	c := newTestClient(t, "events", "events.dlq")
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true,
	}))
	if err := c.Produce(ctx, "events", "k", []byte("traced"), nil); err != nil {
		t.Fatal(err)
	}

	got := make(chan trace.TraceID, 1)
	startConsumer(t, c, opts("g"), func(ctx context.Context, r *Record) error {
		got <- trace.SpanContextFromContext(ctx).TraceID()
		return nil
	})
	select {
	case id := <-got:
		if id != traceID {
			t.Fatalf("handler trace id = %s, want %s", id, traceID)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("record not consumed")
	}
}
