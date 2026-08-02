// Command rjs-sdk-bench measures the native SDK data path for release
// comparison tests. It is not an operational CLI.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	rabbitjetstream "github.com/chennqqi/rabbit-jetstream-go"
	"github.com/nats-io/nats.go"
)

type report struct {
	Schema                   string  `json:"schema"`
	ClientMode               string  `json:"client_mode"`
	NATSVersion              string  `json:"nats_version"`
	GOOS                     string  `json:"goos"`
	GOARCH                   string  `json:"goarch"`
	CPUs                     int     `json:"cpus"`
	Replicas                 int     `json:"replicas"`
	PriorityLevels           int     `json:"priority_levels"`
	PayloadBytes             int     `json:"payload_bytes"`
	Publishers               int     `json:"publishers"`
	RequestedMessages        int64   `json:"requested_messages"`
	Published                int64   `json:"published"`
	Consumed                 int64   `json:"consumed"`
	Missing                  int64   `json:"missing"`
	Duplicates               int64   `json:"duplicates"`
	Corrupt                  int64   `json:"corrupt"`
	DurationSeconds          float64 `json:"duration_seconds"`
	PublishMessagesPerSecond float64 `json:"publish_messages_per_second"`
	ConsumeMessagesPerSecond float64 `json:"consume_messages_per_second"`
	PublishLatencyP50Millis  float64 `json:"publish_latency_p50_millis"`
	PublishLatencyP95Millis  float64 `json:"publish_latency_p95_millis"`
	PublishLatencyP99Millis  float64 `json:"publish_latency_p99_millis"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	server := flag.String("server", nats.DefaultURL, "NATS URLs")
	output := flag.String("output", "", "new JSON report")
	messages := flag.Int64("messages", 10000, "messages to publish")
	payloadBytes := flag.Int("payload-bytes", 1024, "payload bytes, at least 16")
	publishers := flag.Int("publishers", 4, "concurrent synchronous publishers")
	maxPriority := flag.Int("max-priority", 2, "maximum priority")
	replicas := flag.Int("replicas", 3, "Stream replicas")
	timeout := flag.Duration("timeout", 10*time.Minute, "completion timeout")
	flag.Parse()
	if *output == "" || *messages < 1 || *payloadBytes < 16 || *publishers < 1 || *maxPriority < 0 || *maxPriority > 255 || (*replicas != 1 && *replicas != 3 && *replicas != 5) || *timeout <= 0 {
		return errors.New("invalid benchmark arguments")
	}
	nc, err := nats.Connect(*server, nats.Timeout(10*time.Second), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	client, err := rabbitjetstream.New(nc)
	if err != nil {
		return err
	}
	defer nc.Close()
	queue := fmt.Sprintf("sdk_perf_%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := client.EnsurePriorityQueue(ctx, rabbitjetstream.QueueConfig{Name: queue, MaxPriority: *maxPriority, Replicas: *replicas, AckWait: 2 * time.Minute}); err != nil {
		return err
	}
	publisher, err := client.Publisher(queue, "benchmark", *maxPriority)
	if err != nil {
		return err
	}
	consumer, err := client.Consumer(rabbitjetstream.ConsumerConfig{Queue: queue, MaxPriority: *maxPriority, ProbeTimeout: time.Millisecond, MaxInFlightMessages: 1, MaxInFlightBytes: int64(*payloadBytes) * 2})
	if err != nil {
		return err
	}

	started := time.Now()
	consumeStarted := time.Now()
	var consumed, duplicates, corrupt atomic.Int64
	consumeErr := make(chan error, 1)
	go func() {
		seen := make([]bool, *messages)
		for consumed.Load() < *messages {
			message, err := consumer.Next(ctx)
			if err != nil {
				consumeErr <- err
				return
			}
			if len(message.Data) != *payloadBytes {
				corrupt.Add(1)
			} else {
				id := int64(binary.BigEndian.Uint64(message.Data[:8]))
				if id < 0 || id >= *messages {
					corrupt.Add(1)
				} else if seen[id] {
					duplicates.Add(1)
				} else {
					seen[id] = true
				}
			}
			if err := message.Ack(); err != nil {
				consumeErr <- err
				return
			}
			consumed.Add(1)
		}
		consumeErr <- nil
	}()

	jobs := make(chan int64, *publishers*2)
	var published atomic.Int64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, *messages)
	publishErr := make(chan error, 1)
	var workers sync.WaitGroup
	for range *publishers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range jobs {
				payload := make([]byte, *payloadBytes)
				binary.BigEndian.PutUint64(payload[:8], uint64(id))
				begin := time.Now()
				_, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{ID: fmt.Sprintf("sdk-perf-%d", id), Priority: int(id % int64(*maxPriority+1)), Data: payload})
				if err != nil {
					select {
					case publishErr <- err:
					default:
					}
					return
				}
				latencyMu.Lock()
				latencies = append(latencies, time.Since(begin))
				latencyMu.Unlock()
				published.Add(1)
			}
		}()
	}
	for id := int64(0); id < *messages; id++ {
		jobs <- id
	}
	close(jobs)
	workers.Wait()
	publishFinished := time.Now()
	select {
	case err := <-publishErr:
		return err
	default:
	}
	if err := <-consumeErr; err != nil {
		return err
	}
	finished := time.Now()
	missing := *messages - consumed.Load() + duplicates.Load()
	if published.Load() != *messages || missing != 0 || duplicates.Load() != 0 || corrupt.Load() != 0 {
		return fmt.Errorf("integrity failed: published=%d consumed=%d missing=%d duplicates=%d corrupt=%d", published.Load(), consumed.Load(), missing, duplicates.Load(), corrupt.Load())
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(value float64) float64 {
		index := int(float64(len(latencies)-1) * value)
		return float64(latencies[index]) / float64(time.Millisecond)
	}
	value := report{
		Schema: "rabbit-jetstream.io/client-performance-report/v1alpha1", ClientMode: "rabbit-jetstream-go", NATSVersion: nc.ConnectedServerVersion(),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CPUs: runtime.NumCPU(), Replicas: *replicas, PriorityLevels: *maxPriority + 1,
		PayloadBytes: *payloadBytes, Publishers: *publishers, RequestedMessages: *messages, Published: published.Load(), Consumed: consumed.Load(),
		Missing: missing, Duplicates: duplicates.Load(), Corrupt: corrupt.Load(), DurationSeconds: finished.Sub(started).Seconds(),
		PublishMessagesPerSecond: float64(*messages) / publishFinished.Sub(started).Seconds(), ConsumeMessagesPerSecond: float64(*messages) / finished.Sub(consumeStarted).Seconds(),
		PublishLatencyP50Millis: percentile(.50), PublishLatencyP95Millis: percentile(.95), PublishLatencyP99Millis: percentile(.99),
	}
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encodeErr := json.NewEncoder(file).Encode(value)
	closeErr := file.Close()
	if encodeErr != nil {
		return encodeErr
	}
	return closeErr
}
