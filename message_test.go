package rabbitjetstream

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

// fakeMsg embeds jsapi.Msg so unimplemented methods panic; tests override only
// the behavior under test.
type fakeMsg struct {
	jsapi.Msg
	header      nats.Header
	data        []byte
	metadata    *jsapi.MsgMetadata
	metadataErr error
	nakDelay    time.Duration
	nakDelayN   int
	termReason  string
	termReasonN int
}

func (m *fakeMsg) Headers() nats.Header { return m.header }
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Metadata() (*jsapi.MsgMetadata, error) {
	return m.metadata, m.metadataErr
}
func (m *fakeMsg) NakWithDelay(delay time.Duration) error {
	m.nakDelayN++
	m.nakDelay = delay
	return nil
}
func (m *fakeMsg) TermWithReason(reason string) error {
	m.termReasonN++
	m.termReason = reason
	return nil
}

// fakeConsumer embeds jsapi.Consumer and only stubs Info.
type fakeConsumer struct {
	jsapi.Consumer
	info *jsapi.ConsumerInfo
	err  error
}

func (f *fakeConsumer) Info(context.Context) (*jsapi.ConsumerInfo, error) {
	return f.info, f.err
}

func contractHeader(queue string, priority int) nats.Header {
	header := nats.Header{}
	header.Set(HeaderMessageID, "id-1")
	header.Set(HeaderContractVersion, ContractVersion)
	header.Set(HeaderQueue, queue)
	header.Set(HeaderPriority, strconv.Itoa(priority))
	return header
}

func testConsumer() *Consumer {
	return &Consumer{config: ConsumerConfig{Queue: "orders", MaxInFlightBytes: 1 << 20}}
}

func TestMessageDeliveriesFromBrokerMetadata(t *testing.T) {
	consumer := testConsumer()
	raw := &fakeMsg{
		header:   contractHeader("orders", 2),
		data:     []byte("payload"),
		metadata: &jsapi.MsgMetadata{NumDelivered: 3},
	}
	message, err := consumer.wrap(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := message.Deliveries(); got != 3 {
		t.Fatalf("Deliveries() = %d, want 3", got)
	}
}

func TestWrapRejectsUnreadableMetadata(t *testing.T) {
	consumer := testConsumer()
	raw := &fakeMsg{
		header:      contractHeader("orders", 0),
		data:        []byte("payload"),
		metadataErr: errors.New("metadata token mismatch"),
	}
	if _, err := consumer.wrap(raw, 0); err == nil {
		t.Fatal("wrap accepted a message without readable metadata")
	}
}

func TestMessageNakWithDelaySettlesOnce(t *testing.T) {
	consumer := testConsumer()
	raw := &fakeMsg{
		header:   contractHeader("orders", 1),
		data:     []byte("payload"),
		metadata: &jsapi.MsgMetadata{NumDelivered: 1},
	}
	message, err := consumer.wrap(raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := message.NakWithDelay(50 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if raw.nakDelayN != 1 || raw.nakDelay != 50*time.Millisecond {
		t.Fatalf("NakWithDelay forwarded = (%d, %v), want (1, 50ms)", raw.nakDelayN, raw.nakDelay)
	}
	if got := consumer.flightCount; got != 0 {
		t.Fatalf("flightCount = %d, want 0", got)
	}
	if err := message.NakWithDelay(time.Second); err != nil {
		t.Fatal(err)
	}
	if raw.nakDelayN != 1 {
		t.Fatalf("second settle reached the broker: nakDelayN = %d", raw.nakDelayN)
	}
}

func TestMessageTermWithReasonSettles(t *testing.T) {
	consumer := testConsumer()
	raw := &fakeMsg{
		header:   contractHeader("orders", 0),
		data:     []byte("payload"),
		metadata: &jsapi.MsgMetadata{NumDelivered: 2},
	}
	message, err := consumer.wrap(raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := message.TermWithReason("poison: schema rejected"); err != nil {
		t.Fatal(err)
	}
	if raw.termReasonN != 1 || raw.termReason != "poison: schema rejected" {
		t.Fatalf("TermWithReason forwarded = (%d, %q)", raw.termReasonN, raw.termReason)
	}
	if got := consumer.flightCount; got != 0 {
		t.Fatalf("flightCount = %d, want 0", got)
	}
}

func TestConsumerPendingSumsPriorities(t *testing.T) {
	consumer := &Consumer{
		client: &Client{nc: &nats.Conn{}},
		subs: map[int]jsapi.Consumer{
			0: &fakeConsumer{info: &jsapi.ConsumerInfo{NumPending: 2}},
			1: &fakeConsumer{info: &jsapi.ConsumerInfo{NumPending: 5}},
		},
	}
	pending, err := consumer.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pending != 7 {
		t.Fatalf("Pending() = %d, want 7", pending)
	}
}

func TestConsumerPendingPropagatesInfoError(t *testing.T) {
	consumer := &Consumer{
		client: &Client{nc: &nats.Conn{}},
		subs: map[int]jsapi.Consumer{
			0: &fakeConsumer{err: errors.New("consumer deleted")},
		},
	}
	if _, err := consumer.Pending(context.Background()); err == nil {
		t.Fatal("Pending() swallowed a broker error")
	}
}

func TestConsumerPendingAfterClose(t *testing.T) {
	consumer := &Consumer{client: &Client{nc: &nats.Conn{}}, subs: map[int]jsapi.Consumer{}}
	consumer.closed.Store(true)
	if _, err := consumer.Pending(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pending() after Close = %v, want %v", err, ErrClosed)
	}
}
