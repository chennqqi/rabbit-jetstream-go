package rabbitjetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type QueueConfig struct {
	Name          string
	MaxPriority   int
	MaxAge        time.Duration
	MaxBytes      int64
	MaxMessages   int64
	Replicas      int
	AckWait       time.Duration
	MaxDeliveries int
}

// EnsurePriorityQueue explicitly creates a priority topology for standalone
// deployments and tests. Production control planes should own this operation.
// Existing streams are validated and never silently rewritten.
func (c *Client) EnsurePriorityQueue(ctx context.Context, config QueueConfig) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	if err := ValidateQueueName(config.Name); err != nil {
		return err
	}
	if err := ValidatePriority(config.MaxPriority); err != nil {
		return fmt.Errorf("max priority: %w", err)
	}
	stream, _ := StreamName(config.Name)
	subjects := make([]string, 0, config.MaxPriority+1)
	for priority := MinPriority; priority <= config.MaxPriority; priority++ {
		subject, _ := PrioritySubject(config.Name, priority)
		subjects = append(subjects, subject)
	}
	info, err := c.js.StreamInfo(stream, nats.Context(ctx))
	if errors.Is(err, nats.ErrStreamNotFound) {
		replicas := config.Replicas
		if replicas == 0 {
			replicas = 1
		}
		_, err = c.js.AddStream(&nats.StreamConfig{
			Name: stream, Subjects: subjects, Retention: nats.WorkQueuePolicy,
			Storage: nats.FileStorage, MaxAge: config.MaxAge, MaxBytes: config.MaxBytes,
			MaxMsgs: config.MaxMessages, Replicas: replicas, Discard: nats.DiscardOld,
		}, nats.Context(ctx))
		if err != nil {
			return fmt.Errorf("create stream %s: %w", stream, err)
		}
	} else if err != nil {
		return fmt.Errorf("inspect stream %s: %w", stream, err)
	} else if info.Config.Retention != nats.WorkQueuePolicy {
		return fmt.Errorf("stream %s is incompatible: retention must be WorkQueue", stream)
	} else if err := validateStreamSubjects(info.Config.Subjects, subjects); err != nil {
		return fmt.Errorf("stream %s is incompatible: %w", stream, err)
	}
	ackWait := config.AckWait
	if ackWait == 0 {
		ackWait = 30 * time.Second
	}
	maxDeliver := config.MaxDeliveries
	if maxDeliver == 0 {
		maxDeliver = -1
	}
	for priority, subject := range subjects {
		durable, _ := PriorityConsumerName(config.Name, priority)
		consumerInfo, err := c.js.ConsumerInfo(stream, durable, nats.Context(ctx))
		if errors.Is(err, nats.ErrConsumerNotFound) {
			consumerInfo, err = c.js.AddConsumer(stream, &nats.ConsumerConfig{
				Durable: durable, FilterSubject: subject, AckPolicy: nats.AckExplicitPolicy,
				AckWait: ackWait, MaxDeliver: maxDeliver, ReplayPolicy: nats.ReplayInstantPolicy,
			}, nats.Context(ctx))
		}
		if err != nil {
			return fmt.Errorf("ensure consumer %s: %w", durable, err)
		}
		if consumerInfo.Config.FilterSubject != subject || consumerInfo.Config.AckPolicy != nats.AckExplicitPolicy {
			return fmt.Errorf("consumer %s is incompatible: expected filter %s with explicit ack", durable, subject)
		}
	}
	return nil
}

func validateStreamSubjects(actual, required []string) error {
	set := make(map[string]struct{}, len(actual))
	for _, subject := range actual {
		set[subject] = struct{}{}
	}
	for _, subject := range required {
		if _, ok := set[subject]; !ok {
			return fmt.Errorf("missing subject %s", subject)
		}
	}
	return nil
}
