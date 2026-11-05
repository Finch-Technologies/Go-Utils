package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kgo"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
)

// produceTimeout bounds how long Produce keeps retrying a record before failing, so a
// broker outage surfaces as an error instead of a hung request.
const produceTimeout = 30 * time.Second

// Client produces records and runs consumers. It is safe for concurrent use. Close it
// when done.
type Client struct {
	cfg      Config
	base     []kgo.Opt // connection options shared by the producer and every consumer
	producer *kgo.Client
}

// New connects a Client. It doesn't contact the brokers until first use; call Ping to
// check connectivity (e.g. in a readiness probe).
func New(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base, err := connectionOpts(ctx, cfg)
	if err != nil {
		return nil, err
	}
	producer, err := kgo.NewClient(append(base,
		kgo.RequiredAcks(kgo.AllISRAcks()), // the default, stated: every in-sync replica
		kgo.RecordDeliveryTimeout(produceTimeout),
	)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create producer: %w", err)
	}
	return &Client{cfg: cfg, base: base, producer: producer}, nil
}

func connectionOpts(ctx context.Context, cfg Config) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
	}
	if cfg.Auth != AuthIAM {
		return opts, nil
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("kafka: load AWS credentials for MSK IAM: %w", err)
	}
	creds := awsCfg.Credentials // cached and refreshed by the SDK
	return append(opts,
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kgo.SASL(awssasl.ManagedStreamingIAM(func(ctx context.Context) (awssasl.Auth, error) {
			c, err := creds.Retrieve(ctx)
			if err != nil {
				return awssasl.Auth{}, fmt.Errorf("retrieve AWS credentials: %w", err)
			}
			return awssasl.Auth{
				AccessKey:    c.AccessKeyID,
				SecretKey:    c.SecretAccessKey,
				SessionToken: c.SessionToken,
				UserAgent:    cfg.ClientID,
			}, nil
		})),
	), nil
}

// Topic returns the full (environment-prefixed) name of a topic.
func (c *Client) Topic(name string) string { return c.cfg.TopicPrefix + "." + name }

// Group returns the full (environment-prefixed) name of a consumer group.
func (c *Client) Group(name string) string { return c.cfg.TopicPrefix + "." + name }

// Produce writes one record to topic (a name without the environment prefix) and waits
// until every in-sync replica has it. The key chooses the partition: records with the
// same key stay in order. The trace context in ctx is added to the headers.
func (c *Client) Produce(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	rec := &kgo.Record{
		Topic:   c.Topic(topic),
		Value:   value,
		Headers: toKgoHeaders(injectTrace(ctx, headers)),
	}
	if key != "" {
		rec.Key = []byte(key)
	}
	if err := c.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce to %s: %w", rec.Topic, err)
	}
	return nil
}

// Ping checks that a broker is reachable (and, with IAM, that authentication works).
func (c *Client) Ping(ctx context.Context) error {
	if err := c.producer.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}

// Close flushes nothing (Produce is synchronous) and closes the producer's connections.
// Consumers stop when their context is cancelled.
func (c *Client) Close() {
	c.producer.Close()
}
