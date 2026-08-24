package main

import (
	"testing"
	"time"
)

func TestWorkload(t *testing.T) {
	for _, test := range []struct {
		name     string
		messages int64
		duration time.Duration
		rate     int
		timeout  time.Duration
		want     int64
		wantErr  bool
	}{
		{name: "messages", messages: 100, timeout: time.Minute, want: 100},
		{name: "duration", duration: 24 * time.Hour, rate: 5000, timeout: 25 * time.Hour, want: 432_000_000},
		{name: "mixed modes", messages: 1, duration: time.Second, rate: 1, timeout: time.Minute, wantErr: true},
		{name: "short timeout", duration: time.Minute, rate: 1, timeout: time.Minute, wantErr: true},
		{name: "rate without duration", messages: 1, rate: 1, timeout: time.Minute, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := workload(test.messages, test.duration, test.rate, test.timeout)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("workload() = %d, %v; want %d, error=%t", got, err, test.want, test.wantErr)
			}
		})
	}
}
