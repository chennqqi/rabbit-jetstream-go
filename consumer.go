package rabbitjetstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

const DefaultPrefetchMessages = 256

type ConsumerConfig struct {
	Queue               string
	MaxPriority         int
	HighPriorityBurst   int
	LowPriorityProbe    int
	ProbeTimeout        time.Duration
	Prefetch            int
	MaxInFlightMessages int
	MaxInFlightBytes    int64
}

type Consumer struct {
	client      *Client
	config      ConsumerConfig
	scheduler   *Scheduler
	probeOrder  []int
	subs        map[int]jsapi.Consumer
	buffered    map[int]chan jsapi.Msg
	consumeCtxs []jsapi.ConsumeContext
	notify      chan struct{}
	consumeErr  chan error
	done        chan struct{}
	nexting     atomic.Bool
	closed      atomic.Bool
	primed      bool
	flightMu    sync.Mutex
	flightCount int
	flightBytes int64
}

type Message struct {
	ID       string
	Queue    string
	Priority int
	Data     []byte
	Header   nats.Header

	raw      jsapi.Msg
	owner    *Consumer
	settleMu sync.Mutex
	settled  bool
}

func (c *Client) Consumer(config ConsumerConfig) (*Consumer, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := ValidateQueueName(config.Queue); err != nil {
		return nil, err
	}
	if config.ProbeTimeout <= 0 {
		config.ProbeTimeout = 25 * time.Millisecond
	}
	if config.MaxInFlightMessages <= 0 {
		config.MaxInFlightMessages = 128
	}
	if config.MaxInFlightBytes <= 0 {
		config.MaxInFlightBytes = 64 << 20
	}
	if config.Prefetch <= 0 {
		config.Prefetch = DefaultPrefetchMessages / (config.MaxPriority + 1)
		if config.Prefetch < 1 {
			config.Prefetch = 1
		}
	}
	if config.Prefetch > 4096 {
		return nil, fmt.Errorf("prefetch %d exceeds maximum 4096", config.Prefetch)
	}
	scheduler, err := NewScheduler(SchedulerConfig{
		MaxPriority: config.MaxPriority, HighPriorityBurst: config.HighPriorityBurst,
		LowPriorityProbe: config.LowPriorityProbe,
	})
	if err != nil {
		return nil, err
	}
	stream, _ := StreamName(config.Queue)
	consumer := &Consumer{
		client: c, config: config, scheduler: scheduler,
		subs: make(map[int]jsapi.Consumer), buffered: make(map[int]chan jsapi.Msg),
		notify: make(chan struct{}, 1), consumeErr: make(chan error, 1), done: make(chan struct{}),
	}
	bindCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for priority := MinPriority; priority <= config.MaxPriority; priority++ {
		durable, _ := PriorityConsumerName(config.Queue, priority)
		sub, err := c.jsapi.Consumer(bindCtx, stream, durable)
		if err != nil {
			return nil, fmt.Errorf("bind consumer %s: %w", durable, err)
		}
		consumer.subs[priority] = sub
		consumer.buffered[priority] = make(chan jsapi.Msg, config.Prefetch)
	}
	for priority := MinPriority; priority <= config.MaxPriority; priority++ {
		durable, _ := PriorityConsumerName(config.Queue, priority)
		sub := consumer.subs[priority]
		priority := priority
		consumeCtx, err := sub.Consume(func(message jsapi.Msg) {
			select {
			case consumer.buffered[priority] <- message:
				select {
				case consumer.notify <- struct{}{}:
				default:
				}
			case <-consumer.done:
				_ = message.Nak()
			}
		}, jsapi.PullMaxMessages(config.Prefetch), jsapi.ConsumeErrHandler(func(_ jsapi.ConsumeContext, err error) {
			select {
			case consumer.consumeErr <- fmt.Errorf("consume priority %d: %w", priority, err):
			default:
			}
			select {
			case consumer.notify <- struct{}{}:
			default:
			}
		}))
		if err != nil {
			close(consumer.done)
			for _, opened := range consumer.consumeCtxs {
				opened.Stop()
			}
			return nil, fmt.Errorf("start consumer %s: %w", durable, err)
		}
		consumer.consumeCtxs = append(consumer.consumeCtxs, consumeCtx)
	}
	return consumer, nil
}

// Next returns one message. A Consumer permits one concurrent Next call;
// parallelism belongs in handlers after delivery.
func (c *Consumer) Next(ctx context.Context) (*Message, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if !c.nexting.CompareAndSwap(false, true) {
		return nil, ErrConcurrentNext
	}
	defer c.nexting.Store(false)
	if err := c.client.checkOpen(); err != nil {
		return nil, err
	}
	if !c.primed {
		primeWait := c.config.ProbeTimeout
		if primeWait < 10*time.Millisecond {
			primeWait = 10 * time.Millisecond
		}
		timer := time.NewTimer(primeWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case err := <-c.consumeErr:
			timer.Stop()
			return nil, err
		case <-timer.C:
			c.primed = true
		}
	}
	c.flightMu.Lock()
	full := c.flightCount >= c.config.MaxInFlightMessages
	c.flightMu.Unlock()
	if full {
		return nil, ErrMaxInFlightMessages
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.probeOrder = c.scheduler.OrderInto(c.probeOrder)
		order := c.probeOrder
		for _, priority := range order {
			select {
			case message := <-c.buffered[priority]:
				return c.deliver(message, priority)
			default:
			}
		}
		c.scheduler.RecordEmpty()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-c.consumeErr:
			return nil, err
		case <-c.notify:
			// A short aggregation window lets concurrently active priority
			// consumers fill their buffers before the next ordered scan.
			timer := time.NewTimer(c.config.ProbeTimeout)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case err := <-c.consumeErr:
				timer.Stop()
				return nil, err
			case <-timer.C:
			}
		}
	}
}

func (c *Consumer) deliver(raw jsapi.Msg, priority int) (*Message, error) {
	message, err := c.wrap(raw, priority)
	if err != nil {
		_ = raw.Nak()
		return nil, err
	}
	c.scheduler.Record(priority)
	return message, nil
}

// Close releases local pull subscriptions without deleting durable consumers.
// It is safe to call more than once. In-flight messages remain subject to their
// normal AckWait and may be redelivered to another Consumer instance.
func (c *Consumer) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.done)
	for _, consumeCtx := range c.consumeCtxs {
		consumeCtx.Stop()
	}
	var joined error
	for priority, messages := range c.buffered {
		for {
			select {
			case message := <-messages:
				if err := message.Nak(); err != nil {
					joined = errors.Join(joined, fmt.Errorf("release buffered priority %d: %w", priority, err))
				}
			default:
				goto drained
			}
		}
	drained:
		delete(c.buffered, priority)
	}
	return joined
}

func (c *Consumer) wrap(raw jsapi.Msg, priority int) (*Message, error) {
	header := raw.Headers()
	parsed, err := strconv.Atoi(header.Get(HeaderPriority))
	if err != nil || parsed != priority {
		return nil, fmt.Errorf("invalid %s header %q for priority %d", HeaderPriority, header.Get(HeaderPriority), priority)
	}
	if header.Get(HeaderContractVersion) != ContractVersion || header.Get(HeaderQueue) != c.config.Queue || header.Get(HeaderMessageID) == "" {
		return nil, fmt.Errorf("message does not satisfy %s contract", ContractVersion)
	}
	data := raw.Data()
	size := int64(len(data))
	c.flightMu.Lock()
	defer c.flightMu.Unlock()
	if c.flightBytes+size > c.config.MaxInFlightBytes && c.flightCount > 0 {
		return nil, ErrMaxInFlightBytes
	}
	c.flightCount++
	c.flightBytes += size
	return &Message{ID: header.Get(HeaderMessageID), Queue: c.config.Queue, Priority: priority, Data: data, Header: cloneHeader(header), raw: raw, owner: c}, nil
}

func (m *Message) Ack() error        { return m.settle(func() error { return m.raw.Ack() }) }
func (m *Message) Nak() error        { return m.settle(func() error { return m.raw.Nak() }) }
func (m *Message) Term() error       { return m.settle(func() error { return m.raw.Term() }) }
func (m *Message) InProgress() error { return m.raw.InProgress() }

func (m *Message) settle(action func() error) error {
	m.settleMu.Lock()
	defer m.settleMu.Unlock()
	if m.settled {
		return nil
	}
	if err := action(); err != nil {
		return err
	}
	m.settled = true
	m.owner.flightMu.Lock()
	m.owner.flightCount--
	m.owner.flightBytes -= int64(len(m.Data))
	m.owner.flightMu.Unlock()
	return nil
}
