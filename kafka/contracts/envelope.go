// Package contracts is a versioned JSON envelope for messages services exchange over
// Kafka. Services define their own topic names, message types and payload structs, and
// wrap each payload in an Envelope.
package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope wraps every message.
type Envelope[T any] struct {
	// ID identifies the message (UUIDv7), for logs and deduplication.
	ID string `json:"id"`
	// Type names the message, e.g. "orders.order.placed".
	Type string `json:"type"`
	// Version is the schema version of Data. Consumers reject versions they don't know.
	Version int `json:"version"`
	// Source is the producing service, e.g. "orders".
	Source string `json:"source"`
	// Time is when the message was created (UTC).
	Time time.Time `json:"time"`
	// Tenant is the tenant slug, for logs and tracing only: never for authorisation.
	Tenant string `json:"tenant,omitempty"`
	Data   T      `json:"data"`
}

// New builds an envelope with a fresh ID and the current time.
func New[T any](typ string, version int, source, tenant string, data T) Envelope[T] {
	return Envelope[T]{
		ID:      uuid.Must(uuid.NewV7()).String(),
		Type:    typ,
		Version: version,
		Source:  source,
		Time:    time.Now().UTC(),
		Tenant:  tenant,
		Data:    data,
	}
}

// Encode marshals an envelope to JSON.
func Encode[T any](e Envelope[T]) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", e.Type, err)
	}
	return b, nil
}

// Errors from Decode. Both mean retrying can't help: consumers should dead-letter the
// message (kafka.Permanent).
var (
	ErrMalformed          = errors.New("malformed message")
	ErrUnsupportedVersion = errors.New("unsupported message version")
)

// Header is the part of an envelope that identifies the message, without its data.
type Header struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Version int    `json:"version"`
	Source  string `json:"source"`
	Tenant  string `json:"tenant,omitempty"`
}

// Peek reads the envelope header, to choose how to decode the data.
func Peek(b []byte) (Header, error) {
	var h Header
	if err := json.Unmarshal(b, &h); err != nil {
		return h, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if h.Type == "" || h.Version == 0 {
		return h, fmt.Errorf("%w: missing type or version", ErrMalformed)
	}
	return h, nil
}

// Decode unmarshals an envelope of the expected type, accepting only the given
// versions of its data.
func Decode[T any](b []byte, typ string, versions ...int) (Envelope[T], error) {
	var e Envelope[T]
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if e.Type != typ {
		return e, fmt.Errorf("%w: type %q, want %q", ErrMalformed, e.Type, typ)
	}
	for _, v := range versions {
		if e.Version == v {
			return e, nil
		}
	}
	return e, fmt.Errorf("%w: %s version %d", ErrUnsupportedVersion, typ, e.Version)
}
