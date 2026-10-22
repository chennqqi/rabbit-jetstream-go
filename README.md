# rabbit-jetstream-go

[English](README.md) | [简体中文](README.zh-CN.md)

Native Go SDK for the `rabbit-jetstream` server distribution. It implements
the published `v1alpha1` contract without modifying NATS or JetStream. The
first production release targets RabbitMQ priority-queue functionality through
this SDK; the SDK is not AMQP-based and full RabbitMQ/AMQP wire compatibility
belongs to the server project's future roadmap.

The v0.1 production-qualified profile supports up to eight priority levels
(`MaxPriority <= 7`) on Linux/AMD64 with a three-node, three-replica
JetStream cluster. Larger values remain experimental until separately
qualified.

The SDK publishes each priority to `rjs.q.{queue}.p.{priority}`, waits for a
JetStream PubAck, and consumes from one durable pull consumer per priority.
Each durable priority consumer is continuously prefetched into a bounded local
buffer. `Next` scans those buffers from high to low without a network round trip
per empty priority. Its bounded strict-priority scheduler prevents permanent
starvation: after a configured high-priority burst it probes lower levels before
resuming.

```go
client, err := rabbitjetstream.Connect(nats.DefaultURL)
if err != nil { /* handle */ }
defer client.Close()

publisher, _ := client.Publisher("orders", "queue-plan-revision", 9)
ack, err := publisher.Publish(ctx, rabbitjetstream.PublishMessage{
    ID: "order-42", Priority: 9, Data: payload,
})
```

Production topology should be owned by the `rabbit-jetstream` control plane.
`EnsurePriorityQueue` is an explicit helper for standalone deployments and
tests; it validates existing resources rather than rewriting them. Standalone
topologies have no control-plane revision: pass an empty policy revision to
`Client.Publisher` to omit the revision header.

On the consume path, `Message.NakWithDelay` schedules per-message delayed
redelivery for bounded backoff, `Message.Deliveries` exposes the
broker-authoritative delivery count, `Message.TermWithReason` records an
auditable quarantine cause, and `Consumer.Pending` reports the broker-side
ready backlog across all bound priorities.

## Development

```text
go test ./...                         # fast unit tests
go test -race ./...                   # Linux race detection
(cd test/fault && go test ./...)       # broker restart, TCP delay and disconnect recovery
./scripts/test-integration.sh         # Linux + Docker Desktop
pwsh ./scripts/test-integration.ps1   # Windows + Docker Desktop
go run ./cmd/rjs-sdk-bench --help     # release comparison workload
```

CI runs the full integration suite under the Linux race detector and rejects
coverage below 70%. New reliability behavior should raise that baseline.

Delivery is at least once. Ack only after application work is durable, retain
stable message IDs for deduplication, and size in-flight limits for the process
memory budget.

`ConsumerConfig.Prefetch` is the per-priority local buffer size. Zero divides a
256-message default budget across the configured priority levels (for example,
32 messages per level for eight levels). Prefetched lower-priority messages may
already be assigned to this SDK process when a new higher-priority message
arrives, matching the practical prefetch boundary of RabbitMQ priority queues.
Call `Consumer.Close` during graceful shutdown so buffered messages are NAKed
for prompt redelivery. Increase prefetch only after measuring process memory and
priority latency with the production payload distribution.
