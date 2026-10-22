package rabbitjetstream

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
)

// fakeJSContext embeds nats.JetStreamContext and captures the published
// message so tests can inspect contract headers without a broker.
type fakeJSContext struct {
	nats.JetStreamContext
	msg *nats.Msg
}

func (f *fakeJSContext) PublishMsg(msg *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	f.msg = msg
	return &nats.PubAck{Stream: "RJSQ_orders", Sequence: 1}, nil
}

func TestPublisherEmptyPolicyRevisionOmitsHeader(t *testing.T) {
	client := &Client{nc: &nats.Conn{}}
	publisher, err := client.Publisher("orders", "", 3)
	if err != nil {
		t.Fatalf("Publisher() rejected an empty policy revision: %v", err)
	}
	fjs := &fakeJSContext{}
	client.js = fjs
	if _, err := publisher.Publish(context.Background(), PublishMessage{ID: "m1", Priority: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if _, present := fjs.msg.Header[HeaderPolicyRevision]; present {
		t.Fatalf("empty policy revision still set %s", HeaderPolicyRevision)
	}
	if got := fjs.msg.Header.Get(HeaderContractVersion); got != ContractVersion {
		t.Fatalf("%s = %q, want %q", HeaderContractVersion, got, ContractVersion)
	}
}

func TestPublisherPolicyRevisionSetsHeader(t *testing.T) {
	client := &Client{nc: &nats.Conn{}}
	publisher, err := client.Publisher("orders", "rev-7", 3)
	if err != nil {
		t.Fatal(err)
	}
	fjs := &fakeJSContext{}
	client.js = fjs
	if _, err := publisher.Publish(context.Background(), PublishMessage{ID: "m1", Priority: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if got := fjs.msg.Header.Get(HeaderPolicyRevision); got != "rev-7" {
		t.Fatalf("%s = %q, want %q", HeaderPolicyRevision, got, "rev-7")
	}
}
