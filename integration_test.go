//go:build integration

package rabbitjetstream

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

func integrationClient(t *testing.T) *Client {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	client, err := Connect(url, nats.Timeout(5*time.Second), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func provisionTestQueue(t *testing.T, client *Client, suffix string, maxPriority int, ackWait time.Duration) string {
	t.Helper()
	queue := fmt.Sprintf("sdk_%s_%d", suffix, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.EnsurePriorityQueue(ctx, QueueConfig{Name: queue, MaxPriority: maxPriority, AckWait: ackWait}); err != nil {
		t.Fatal(err)
	}
	stream, _ := StreamName(queue)
	t.Cleanup(func() { _ = client.js.DeleteStream(stream) })
	return queue
}

func TestIntegrationPriorityFairnessAndPubAck(t *testing.T) {
	client := integrationClient(t)
	queue := provisionTestQueue(t, client, "priority", 3, time.Second)
	publisher, err := client.Publisher(queue, "revision-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, message := range []PublishMessage{
		{ID: "low", Priority: 0, Data: []byte("low")},
		{ID: "high-1", Priority: 3, Data: []byte("high")},
		{ID: "high-2", Priority: 3, Data: []byte("high")},
		{ID: "high-3", Priority: 3, Data: []byte("high")},
	} {
		ack, err := publisher.Publish(ctx, message)
		if err != nil {
			t.Fatal(err)
		}
		if ack.Stream == "" || ack.Sequence == 0 {
			t.Fatalf("invalid PubAck: %+v", ack)
		}
	}
	consumer, err := client.Consumer(ConsumerConfig{
		Queue: queue, MaxPriority: 3, HighPriorityBurst: 2, LowPriorityProbe: 1,
		ProbeTimeout: 10 * time.Millisecond, MaxInFlightMessages: 8, MaxInFlightBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"high-1", "high-2", "low", "high-3"}
	for index, id := range want {
		message, err := consumer.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != id {
			t.Fatalf("delivery %d = %q, want %q", index, message.ID, id)
		}
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegrationEightPriorityPrefetchOrder(t *testing.T) {
	client := integrationClient(t)
	queue := provisionTestQueue(t, client, "eight_priority", 7, time.Second)
	publisher, err := client.Publisher(queue, "revision-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for priority := 0; priority <= 7; priority++ {
		for sequence := 0; sequence < 4; sequence++ {
			id := fmt.Sprintf("p%d-%d", priority, sequence)
			if _, err := publisher.Publish(ctx, PublishMessage{ID: id, Priority: priority, Data: []byte(id)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	consumer, err := client.Consumer(ConsumerConfig{
		Queue: queue, MaxPriority: 7, HighPriorityBurst: 32, LowPriorityProbe: 1,
		ProbeTimeout: time.Millisecond, Prefetch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	for priority := 7; priority >= 0; priority-- {
		for sequence := 0; sequence < 4; sequence++ {
			message, err := consumer.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("p%d-%d", priority, sequence)
			if message.ID != want {
				t.Fatalf("delivery at priority %d sequence %d = %q, want %q", priority, sequence, message.ID, want)
			}
			if err := message.Ack(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestIntegrationCloseReleasesPrefetchedMessages(t *testing.T) {
	client := integrationClient(t)
	queue := provisionTestQueue(t, client, "prefetch_close", 0, 10*time.Second)
	publisher, _ := client.Publisher(queue, "revision-1", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for sequence := 0; sequence < 4; sequence++ {
		id := fmt.Sprintf("buffered-%d", sequence)
		if _, err := publisher.Publish(ctx, PublishMessage{ID: id, Data: []byte(id)}); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := client.Consumer(ConsumerConfig{Queue: queue, MaxPriority: 0, Prefetch: 4})
	if err != nil {
		t.Fatal(err)
	}
	first, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := client.Consumer(ConsumerConfig{Queue: queue, MaxPriority: 0, Prefetch: 1})
	if err != nil {
		t.Fatal(err)
	}
	redelivered := make(map[string]bool, 3)
	for sequence := 1; sequence < 4; sequence++ {
		message, err := reopened.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		redelivered[message.ID] = true
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	for sequence := 1; sequence < 4; sequence++ {
		want := fmt.Sprintf("buffered-%d", sequence)
		if !redelivered[want] {
			t.Errorf("buffered message %q was not redelivered", want)
		}
	}
}

func TestIntegrationBackpressureAndRedelivery(t *testing.T) {
	client := integrationClient(t)
	queue := provisionTestQueue(t, client, "redelivery", 0, 150*time.Millisecond)
	publisher, _ := client.Publisher(queue, "revision-1", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := publisher.Publish(ctx, PublishMessage{ID: "redeliver-me", Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	consumer, err := client.Consumer(ConsumerConfig{
		Queue: queue, MaxPriority: 0, ProbeTimeout: 10 * time.Millisecond,
		MaxInFlightMessages: 1, MaxInFlightBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(ctx); !errors.Is(err, ErrMaxInFlightMessages) {
		t.Fatalf("Next at capacity = %v, want %v", err, ErrMaxInFlightMessages)
	}
	if err := first.Nak(); err != nil {
		t.Fatal(err)
	}
	redelivered, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if redelivered.ID != first.ID {
		t.Fatalf("redelivered ID = %q, want %q", redelivered.ID, first.ID)
	}
	if err := redelivered.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrationDelayedSettleAndBacklog covers the settle and backlog
// primitives adapters need: NakWithDelay delays per-message redelivery,
// Deliveries exposes the broker delivery count, TermWithReason quarantines
// permanently, Pending reports broker-side ready depth, and a Publisher built
// with an empty policy revision works against EnsurePriorityQueue topology.
func TestIntegrationDelayedSettleAndBacklog(t *testing.T) {
	client := integrationClient(t)
	queue := provisionTestQueue(t, client, "delay_nak", 0, 500*time.Millisecond)
	publisher, err := client.Publisher(queue, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := publisher.Publish(ctx, PublishMessage{ID: "delayed", Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	// Bind without pulling: Pending must report the broker ready depth
	// deterministically, before any Consumer starts prefetching.
	stream, _ := StreamName(queue)
	durable, _ := PriorityConsumerName(queue, 0)
	sub, err := client.jsapi.Consumer(ctx, stream, durable)
	if err != nil {
		t.Fatal(err)
	}
	observer := &Consumer{client: client, subs: map[int]jsapi.Consumer{0: sub}}
	pending, err := observer.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("Pending() before consume = %d, want 1", pending)
	}
	consumer, err := client.Consumer(ConsumerConfig{Queue: queue, MaxPriority: 0, ProbeTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	first, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Deliveries(); got != 1 {
		t.Fatalf("first Deliveries() = %d, want 1", got)
	}
	start := time.Now()
	if err := first.NakWithDelay(300 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	redelivered, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("delayed redelivery returned after %v, want >= ~300ms", elapsed)
	}
	if redelivered.ID != first.ID {
		t.Fatalf("redelivered ID = %q, want %q", redelivered.ID, first.ID)
	}
	if got := redelivered.Deliveries(); got != 2 {
		t.Fatalf("second Deliveries() = %d, want 2", got)
	}
	if err := redelivered.TermWithReason("poison: rejected by test"); err != nil {
		t.Fatal(err)
	}
	short, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	defer stop()
	if _, err := consumer.Next(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next after TermWithReason = %v, want deadline exceeded", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		pending, err := consumer.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Pending() = %d after drain, want 0", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIntegrationManagedPriorityQueue binds only to resources created by the
// rabbit-jetstream control plane. It intentionally never calls
// EnsurePriorityQueue, making it a cross-repository contract test.
func TestIntegrationManagedPriorityQueue(t *testing.T) {
	queue := os.Getenv("RJS_MANAGED_QUEUE")
	revision := os.Getenv("RJS_MANAGED_REVISION")
	if queue == "" || revision == "" {
		t.Skip("RJS_MANAGED_QUEUE and RJS_MANAGED_REVISION are required")
	}
	client := integrationClient(t)
	publisher, err := client.Publisher(queue, revision, 2)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := client.Consumer(ConsumerConfig{Queue: queue, MaxPriority: 2, ProbeTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("managed-%d-", time.Now().UnixNano())
	for _, message := range []PublishMessage{
		{ID: prefix + "low", Priority: 0, Data: []byte("low")},
		{ID: prefix + "high", Priority: 2, Data: []byte("high")},
	} {
		if _, err := publisher.Publish(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{prefix + "high", prefix + "low"} {
		message, err := consumer.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != want {
			t.Fatalf("managed delivery = %q, want %q", message.ID, want)
		}
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	// Settle and backlog primitives must behave identically on control-plane
	// managed topology. The fixture sets maxDeliver=5, leaving room for a
	// delayed retry.
	if _, err := publisher.Publish(ctx, PublishMessage{ID: prefix + "retry", Priority: 1, Data: []byte("retry")}); err != nil {
		t.Fatal(err)
	}
	retry, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := retry.Deliveries(); got != 1 {
		t.Fatalf("managed first Deliveries() = %d, want 1", got)
	}
	if err := retry.NakWithDelay(200 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	again, err := consumer.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != retry.ID {
		t.Fatalf("managed redelivery ID = %q, want %q", again.ID, retry.ID)
	}
	if got := again.Deliveries(); got != 2 {
		t.Fatalf("managed second Deliveries() = %d, want 2", got)
	}
	if err := again.TermWithReason("contract test quarantine"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		pending, err := consumer.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("managed Pending() = %d after drain, want 0", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestIntegrationConsumerCloseKeepsDurableState(t *testing.T) {
	client := integrationClient(t)
	queue := provisionTestQueue(t, client, "close", 0, time.Second)
	publisher, _ := client.Publisher(queue, "revision-1", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := publisher.Publish(ctx, PublishMessage{ID: "after-close", Data: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	consumer, err := client.Consumer(ConsumerConfig{Queue: queue, MaxPriority: 0, ProbeTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after Close = %v, want %v", err, ErrClosed)
	}
	reopened, err := client.Consumer(ConsumerConfig{Queue: queue, MaxPriority: 0, ProbeTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	message, err := reopened.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != "after-close" {
		t.Fatalf("message ID = %q", message.ID)
	}
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
}
