package rabbitjetstream

import (
	"errors"
	"fmt"
	"testing"

	jsapi "github.com/nats-io/nats.go/jetstream"
)

func TestIsTerminalConsumeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "consumer deleted", err: jsapi.ErrConsumerDeleted, want: true},
		{name: "wrapped bad request", err: fmt.Errorf("pull: %w", jsapi.ErrBadRequest), want: true},
		{name: "server shutdown", err: errors.New("nats: server shutdown"), want: false},
		{name: "missing heartbeat", err: jsapi.ErrNoHeartbeat, want: false},
		{name: "unclassified recoverable error", err: errors.New("temporary"), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTerminalConsumeError(test.err); got != test.want {
				t.Fatalf("isTerminalConsumeError(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}
