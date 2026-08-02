# rabbit-jetstream-go

Native Go SDK for the `rabbit-jetstream` server distribution. It implements
the published `v1alpha1` contract without modifying NATS or JetStream.

The SDK publishes each priority to `rjs.q.{queue}.p.{priority}`, waits for a
JetStream PubAck, and consumes from one durable pull consumer per priority.
Its bounded strict-priority scheduler prevents permanent starvation: after a
configured high-priority burst it probes lower levels before resuming.

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
tests; it validates existing resources rather than rewriting them.

## Development

```text
go test ./...                         # fast unit tests
go test -race ./...                   # Linux race detection
(cd test/fault && go test ./...)       # broker restart, TCP delay and disconnect recovery
./scripts/test-integration.sh         # Linux + Docker Desktop
pwsh ./scripts/test-integration.ps1   # Windows + Docker Desktop
```

CI runs the full integration suite under the Linux race detector and rejects
coverage below 70%. New reliability behavior should raise that baseline.

Delivery is at least once. Ack only after application work is durable, retain
stable message IDs for deduplication, and size in-flight limits for the process
memory budget.
