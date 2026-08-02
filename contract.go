// Package rabbitjetstream provides the native Go client for rabbit-jetstream.
package rabbitjetstream

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/nats-io/nats.go"
)

func cloneHeader(source nats.Header) nats.Header {
	if source == nil {
		return nil
	}
	clone := make(nats.Header, len(source))
	for key, values := range source {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}

const (
	ContractVersion = "v1alpha1"

	HeaderMessageID       = "Nats-Msg-Id"
	HeaderContractVersion = "Rjs-Contract-Version"
	HeaderQueue           = "Rjs-Queue"
	HeaderPolicyRevision  = "Rjs-Policy-Revision"
	HeaderPriority        = "Rjs-Priority"

	MinPriority = 0
	MaxPriority = 255
)

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func ValidateQueueName(name string) error {
	if !queueNamePattern.MatchString(name) {
		return fmt.Errorf("queue name %q must match %s", name, queueNamePattern)
	}
	return nil
}

func StreamName(queue string) (string, error) {
	if err := ValidateQueueName(queue); err != nil {
		return "", err
	}
	return "RJSQ_" + queue, nil
}

func PrioritySubject(queue string, priority int) (string, error) {
	if err := ValidateQueueName(queue); err != nil {
		return "", err
	}
	if err := ValidatePriority(priority); err != nil {
		return "", err
	}
	return "rjs.q." + queue + ".p." + strconv.Itoa(priority), nil
}

func PriorityConsumerName(queue string, priority int) (string, error) {
	if err := ValidateQueueName(queue); err != nil {
		return "", err
	}
	if err := ValidatePriority(priority); err != nil {
		return "", err
	}
	return "RJSQC_" + queue + "_P" + strconv.Itoa(priority), nil
}

func ValidatePriority(priority int) error {
	if priority < MinPriority || priority > MaxPriority {
		return fmt.Errorf("priority %d is outside [%d,%d]", priority, MinPriority, MaxPriority)
	}
	return nil
}

var (
	ErrClosed              = errors.New("rabbitjetstream: closed")
	ErrNoMessage           = errors.New("rabbitjetstream: no message available")
	ErrConcurrentNext      = errors.New("rabbitjetstream: Next must not be called concurrently")
	ErrMaxInFlightMessages = errors.New("rabbitjetstream: maximum in-flight messages reached")
	ErrMaxInFlightBytes    = errors.New("rabbitjetstream: maximum in-flight bytes reached")
)
