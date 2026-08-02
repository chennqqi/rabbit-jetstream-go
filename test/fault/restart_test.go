package fault_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	rabbitjetstream "github.com/chennqqi/rabbit-jetstream-go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startServer(t *testing.T, options *server.Options) *server.Server {
	t.Helper()
	s, err := server.NewServer(options)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	return s
}

func TestBrokerRestartRecoversPublishAndConsume(t *testing.T) {
	options := &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true}
	broker := startServer(t, options)
	options.Port = broker.Addr().(*net.TCPAddr).Port

	reconnected := make(chan struct{}, 1)
	client, err := rabbitjetstream.Connect(broker.ClientURL(),
		nats.MaxReconnects(-1), nats.ReconnectWait(25*time.Millisecond), nats.Timeout(2*time.Second),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := fmt.Sprintf("sdk_restart_%d", time.Now().UnixNano())
	if err := client.EnsurePriorityQueue(ctx, rabbitjetstream.QueueConfig{Name: queue, MaxPriority: 1}); err != nil {
		t.Fatal(err)
	}
	publisher, err := client.Publisher(queue, "revision-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{ID: "before", Data: []byte("before")}); err != nil {
		t.Fatal(err)
	}

	broker.Shutdown()
	broker.WaitForShutdown()
	broker = startServer(t, options)
	t.Cleanup(func() { broker.Shutdown(); broker.WaitForShutdown() })
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("client did not reconnect")
	}

	if _, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{ID: "after", Priority: 1, Data: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	consumer, err := client.Consumer(rabbitjetstream.ConsumerConfig{Queue: queue, MaxPriority: 1, ProbeTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"after", "before"} {
		message, err := consumer.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != want {
			t.Fatalf("message ID = %q, want %q", message.ID, want)
		}
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
