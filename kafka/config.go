// Package kafka is a small client for service-to-service messaging over Kafka (AWS MSK),
// built on franz-go.
//
// It covers what services need and makes the safe choices for them:
//
//   - Producing waits for every in-sync replica (acks=all) and is idempotent.
//   - Consuming commits a record only after its handler (or the dead-letter publish)
//     succeeded, processes partitions in parallel and records within a partition in
//     order, retries failures with exponential backoff and then sends the record to a
//     dead-letter topic. A record is never skipped silently.
//   - Topic and consumer group names are prefixed with the environment
//     (KAFKA_TOPIC_PREFIX), so environments sharing a cluster can't see each other's
//     messages.
//   - Authentication is MSK IAM over TLS (KAFKA_AUTH=iam, credentials from the default
//     AWS chain: IRSA in a cluster, SSO locally), or none for a local broker.
//   - OpenTelemetry trace context travels in record headers.
//
// Delivery is at least once: handlers must cope with seeing a record more than once.
package kafka

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Authentication modes (KAFKA_AUTH).
const (
	// AuthIAM is MSK IAM authentication (SASL AWS_MSK_IAM) over TLS, port 9098.
	AuthIAM = "iam"
	// AuthNone is a plaintext connection without authentication: local brokers only.
	AuthNone = "none"
)

// Config configures a Client.
type Config struct {
	// Brokers are the bootstrap brokers (KAFKA_BROKERS, comma-separated).
	Brokers []string
	// Auth is AuthIAM or AuthNone (KAFKA_AUTH, default iam).
	Auth string
	// Region is the AWS region used to load credentials for AuthIAM (AWS_REGION).
	Region string
	// TopicPrefix is prepended, with a dot, to every topic and consumer group name
	// (KAFKA_TOPIC_PREFIX), e.g. "dev" turns "orders.order-events" into
	// "dev.orders.order-events". Required, so that environments sharing a cluster
	// stay apart.
	TopicPrefix string
	// ClientID identifies the service to the brokers (and in MSK IAM's user agent).
	ClientID string
}

var prefixPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// ConfigFromEnv reads KAFKA_BROKERS, KAFKA_AUTH, AWS_REGION and KAFKA_TOPIC_PREFIX.
func ConfigFromEnv(clientID string) (Config, error) {
	cfg := Config{
		Brokers:     splitBrokers(os.Getenv("KAFKA_BROKERS")),
		Auth:        os.Getenv("KAFKA_AUTH"),
		Region:      os.Getenv("AWS_REGION"),
		TopicPrefix: os.Getenv("KAFKA_TOPIC_PREFIX"),
		ClientID:    clientID,
	}
	if cfg.Auth == "" {
		cfg.Auth = AuthIAM
	}
	return cfg, cfg.Validate()
}

// Validate reports the first problem with the configuration.
func (c Config) Validate() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("no brokers (KAFKA_BROKERS)"))
	}
	switch c.Auth {
	case AuthIAM:
		if c.Region == "" {
			errs = append(errs, errors.New("KAFKA_AUTH=iam needs a region (AWS_REGION)"))
		}
	case AuthNone:
	default:
		errs = append(errs, fmt.Errorf("unknown auth %q (KAFKA_AUTH must be %s or %s)", c.Auth, AuthIAM, AuthNone))
	}
	if !prefixPattern.MatchString(c.TopicPrefix) {
		errs = append(errs, fmt.Errorf("invalid topic prefix %q (KAFKA_TOPIC_PREFIX: lowercase letters, digits and hyphens, e.g. dev)", c.TopicPrefix))
	}
	if c.ClientID == "" {
		errs = append(errs, errors.New("client id is required"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("kafka config: %w", errors.Join(errs...))
	}
	return nil
}

func splitBrokers(s string) []string {
	var brokers []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return brokers
}
