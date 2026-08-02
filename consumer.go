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
)

type ConsumerConfig struct {
	Queue               string
	MaxPriority         int
	HighPriorityBurst   int
	LowPriorityProbe    int
	ProbeTimeout        time.Duration
	MaxInFlightMessages int
	MaxInFlightBytes    int64
}

type Consumer struct {
	client      *Client
	config      ConsumerConfig
	scheduler   *Scheduler
	probeOrder  []int
	subs        map[int]*nats.Subscription
	nexting     atomic.Bool
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

	raw      *nats.Msg
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
	scheduler, err := NewScheduler(SchedulerConfig{
		MaxPriority: config.MaxPriority, HighPriorityBurst: config.HighPriorityBurst,
		LowPriorityProbe: config.LowPriorityProbe,
	})
	if err != nil {
		return nil, err
	}
	stream, _ := StreamName(config.Queue)
	consumer := &Consumer{client: c, config: config, scheduler: scheduler, subs: make(map[int]*nats.Subscription)}
	for priority := MinPriority; priority <= config.MaxPriority; priority++ {
		subject, _ := PrioritySubject(config.Queue, priority)
		durable, _ := PriorityConsumerName(config.Queue, priority)
		sub, err := c.js.PullSubscribe(subject, durable, nats.Bind(stream, durable))
		if err != nil {
			return nil, fmt.Errorf("bind consumer %s: %w", durable, err)
		}
		consumer.subs[priority] = sub
	}
	return consumer, nil
}

// Next returns one message. A Consumer permits one concurrent Next call;
// parallelism belongs in handlers after delivery.
func (c *Consumer) Next(ctx context.Context) (*Message, error) {
	if !c.nexting.CompareAndSwap(false, true) {
		return nil, ErrConcurrentNext
	}
	defer c.nexting.Store(false)
	if err := c.client.checkOpen(); err != nil {
		return nil, err
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
		found := false
		for _, priority := range order {
			probeCtx, cancel := context.WithTimeout(ctx, c.config.ProbeTimeout)
			messages, err := c.subs[priority].Fetch(1, nats.Context(probeCtx))
			cancel()
			if err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("fetch priority %d: %w", priority, err)
			}
			if len(messages) == 0 {
				continue
			}
			found = true
			message, err := c.wrap(messages[0], priority)
			if err != nil {
				_ = messages[0].Nak()
				return nil, err
			}
			c.scheduler.Record(priority)
			return message, nil
		}
		if !found {
			c.scheduler.RecordEmpty()
		}
	}
}

func (c *Consumer) wrap(raw *nats.Msg, priority int) (*Message, error) {
	parsed, err := strconv.Atoi(raw.Header.Get(HeaderPriority))
	if err != nil || parsed != priority {
		return nil, fmt.Errorf("invalid %s header %q for priority %d", HeaderPriority, raw.Header.Get(HeaderPriority), priority)
	}
	if raw.Header.Get(HeaderContractVersion) != ContractVersion || raw.Header.Get(HeaderQueue) != c.config.Queue || raw.Header.Get(HeaderMessageID) == "" {
		return nil, fmt.Errorf("message does not satisfy %s contract", ContractVersion)
	}
	size := int64(len(raw.Data))
	c.flightMu.Lock()
	defer c.flightMu.Unlock()
	if c.flightBytes+size > c.config.MaxInFlightBytes && c.flightCount > 0 {
		return nil, ErrMaxInFlightBytes
	}
	c.flightCount++
	c.flightBytes += size
	return &Message{ID: raw.Header.Get(HeaderMessageID), Queue: c.config.Queue, Priority: priority, Data: raw.Data, Header: cloneHeader(raw.Header), raw: raw, owner: c}, nil
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
