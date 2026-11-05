package contracts_test

import (
	"errors"
	"testing"

	"github.com/finch-technologies/go-utils/kafka/contracts"
	"github.com/google/uuid"
)

const (
	typeOrderPlaced    = "orders.order.placed"
	typeOrderCancelled = "orders.order.cancelled"
)

type orderPlaced struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

func TestRoundTrip(t *testing.T) {
	in := contracts.New(typeOrderPlaced, 1, "orders", "acme", orderPlaced{OrderID: "o-1", Amount: 1500})
	if id, err := uuid.Parse(in.ID); err != nil || id.Version() != 7 {
		t.Fatalf("id %q is not a UUIDv7", in.ID)
	}
	b, err := contracts.Encode(in)
	if err != nil {
		t.Fatal(err)
	}

	h, err := contracts.Peek(b)
	if err != nil || h.Type != typeOrderPlaced || h.Version != 1 || h.Tenant != "acme" {
		t.Fatalf("Peek = %+v, %v", h, err)
	}
	out, err := contracts.Decode[orderPlaced](b, typeOrderPlaced, 1)
	if err != nil {
		t.Fatal(err)
	}
	if out.Data != in.Data || out.ID != in.ID || !out.Time.Equal(in.Time) {
		t.Fatalf("round trip changed the message:\n in %+v\nout %+v", in, out)
	}
}

func TestDecodeRejects(t *testing.T) {
	ev, _ := contracts.Encode(contracts.New(typeOrderPlaced, 2, "orders", "", orderPlaced{}))
	for name, tc := range map[string]struct {
		in   []byte
		typ  string
		want error
	}{
		"not json":      {[]byte("{"), typeOrderPlaced, contracts.ErrMalformed},
		"wrong type":    {ev, typeOrderCancelled, contracts.ErrMalformed},
		"newer version": {ev, typeOrderPlaced, contracts.ErrUnsupportedVersion},
	} {
		if _, err := contracts.Decode[orderPlaced](tc.in, tc.typ, 1); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
	if _, err := contracts.Peek([]byte(`{"id":"x"}`)); !errors.Is(err, contracts.ErrMalformed) {
		t.Errorf("Peek without type/version: %v", err)
	}
}
