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
