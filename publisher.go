package rabbitjetstream

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

type Publisher struct {
	client         *Client
	queue          string
	policyRevision string
	maxPriority    int
}

type PublishMessage struct {
	ID       string
	Priority int
	Data     []byte
	Header   nats.Header
}

type PublishAck struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

func (c *Client) Publisher(queue, policyRevision string, maxPriority int) (*Publisher, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := ValidateQueueName(queue); err != nil {
		return nil, err
	}
	if policyRevision == "" {
		return nil, fmt.Errorf("policy revision is required")
	}
	if err := ValidatePriority(maxPriority); err != nil {
		return nil, fmt.Errorf("max priority: %w", err)
	}
	return &Publisher{client: c, queue: queue, policyRevision: policyRevision, maxPriority: maxPriority}, nil
}

func (p *Publisher) Publish(ctx context.Context, message PublishMessage) (*PublishAck, error) {
	if err := p.client.checkOpen(); err != nil {
		return nil, err
	}
	if message.ID == "" {
		return nil, fmt.Errorf("message ID is required")
	}
	if err := ValidatePriority(message.Priority); err != nil || message.Priority > p.maxPriority {
		return nil, fmt.Errorf("priority %d exceeds queue maximum %d", message.Priority, p.maxPriority)
	}
	subject, _ := PrioritySubject(p.queue, message.Priority)
	header := cloneHeader(message.Header)
	if header == nil {
		header = make(nats.Header)
	}
	header.Set(HeaderMessageID, message.ID)
	header.Set(HeaderContractVersion, ContractVersion)
	header.Set(HeaderQueue, p.queue)
	header.Set(HeaderPolicyRevision, p.policyRevision)
	header.Set(HeaderPriority, strconv.Itoa(message.Priority))
	ack, err := p.client.js.PublishMsg(&nats.Msg{Subject: subject, Header: header, Data: message.Data}, nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("publish %s: %w", message.ID, err)
	}
	return &PublishAck{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}
