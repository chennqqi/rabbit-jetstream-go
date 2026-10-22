# Native SDK v0.1 API

The v0.1 release candidate freezes the root `rabbitjetstream` package as the supported application API. `Connect` owns a NATS connection; `New` wraps a caller-owned connection. Authentication, TLS, reconnect behavior and callbacks use standard `nats.Option` values.

`Client.Publisher` returns a concurrency-safe publisher. Every `Publish` requires a stable message ID, validates priority against the Queue maximum, overwrites reserved contract headers, and returns only after JetStream PubAck or an error. The policy revision is required for control-plane topologies; pass an empty string for standalone or `EnsurePriorityQueue` test topologies and the revision header is omitted.

`Client.Consumer` binds the contract's durable pull Consumers. One goroutine calls `Next`; applications may dispatch returned messages to bounded workers up to the configured in-flight message and byte limits. Each message must finish with `Ack`, `Nak`, `NakWithDelay`, `Term`, or `TermWithReason`. `NakWithDelay` asks the broker to redeliver after a per-message delay for bounded backoff; `Deliveries` reports the broker-authoritative delivery count, starting at 1, for `MaxDeliveries`-style gates; `TermWithReason` attaches an auditable cause to one-way quarantine (NATS server >= 2.10.4). `Consumer.Pending` returns the broker-side ready backlog summed across the bound priority consumers. `Close` releases local subscriptions but preserves durable broker state and does not settle in-flight messages.

`EnsurePriorityQueue` is an explicit standalone/test helper. Production applications should let the rabbit-jetstream control plane own topology and use only Publisher/Consumer on the data path.

Compatibility is governed by `ContractVersion`. Incompatible resource names, headers or scheduling semantics require a new contract version; additive API changes may remain within SDK v0.1 patch releases.
