package fault_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rabbitjetstream "github.com/chennqqi/rabbit-jetstream-go"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type faultProxy struct {
	listener net.Listener
	target   string
	delayNS  atomic.Int64
	mu       sync.Mutex
	active   map[net.Conn]struct{}
	done     chan struct{}
}

func newFaultProxy(t *testing.T, target string) *faultProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &faultProxy{listener: listener, target: target, active: map[net.Conn]struct{}{}, done: make(chan struct{})}
	go proxy.serve()
	t.Cleanup(proxy.close)
	return proxy
}

func (p *faultProxy) serve() {
	defer close(p.done)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.track(client, upstream)
		go p.forward(client, upstream)
		go p.forward(upstream, client)
	}
}

func (p *faultProxy) forward(destination, source net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		read, err := source.Read(buffer)
		if read > 0 {
			if delay := time.Duration(p.delayNS.Load()); delay > 0 {
				time.Sleep(delay)
			}
			if _, writeErr := destination.Write(buffer[:read]); writeErr != nil {
				err = writeErr
			}
		}
		if err != nil {
			_ = source.Close()
			_ = destination.Close()
			p.untrack(source, destination)
			return
		}
	}
}

func (p *faultProxy) track(connections ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range connections {
		p.active[connection] = struct{}{}
	}
}

func (p *faultProxy) untrack(connections ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range connections {
		delete(p.active, connection)
	}
}

func (p *faultProxy) interrupt() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for connection := range p.active {
		_ = connection.Close()
		delete(p.active, connection)
	}
}

func (p *faultProxy) close() {
	_ = p.listener.Close()
	p.interrupt()
	<-p.done
}

func TestNetworkDelayAndDisconnectRecovery(t *testing.T) {
	broker := startServer(t, &server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	t.Cleanup(func() { broker.Shutdown(); broker.WaitForShutdown() })
	proxy := newFaultProxy(t, broker.Addr().String())
	proxy.delayNS.Store(int64(15 * time.Millisecond))

	reconnected := make(chan struct{}, 1)
	client, err := rabbitjetstream.Connect("nats://"+proxy.listener.Addr().String(),
		nats.MaxReconnects(-1), nats.ReconnectWait(25*time.Millisecond),
		nats.ReconnectHandler(func(*nats.Conn) {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := "network_fault"
	if err := client.EnsurePriorityQueue(ctx, rabbitjetstream.QueueConfig{Name: queue, MaxPriority: 1}); err != nil {
		t.Fatal(err)
	}
	publisher, _ := client.Publisher(queue, "revision-1", 1)
	started := time.Now()
	if _, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{ID: "delayed", Priority: 1, Data: []byte("delayed")}); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < 15*time.Millisecond {
		t.Fatal("network delay was not applied to PubAck path")
	}

	proxy.interrupt()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("client did not reconnect through proxy")
	}
	if _, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{ID: "recovered", Data: []byte("recovered")}); err != nil {
		t.Fatal(err)
	}
	consumer, err := client.Consumer(rabbitjetstream.ConsumerConfig{Queue: queue, MaxPriority: 1, ProbeTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		message, err := consumer.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := message.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
