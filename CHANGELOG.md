# Changelog

## v0.1.0-rc.1

- Publish priority messages with stable IDs and synchronous JetStream PubAck.
- Consume one durable pull Consumer per priority through bounded strict-priority scheduling.
- Expose Ack, Nak, Term and InProgress with at-least-once delivery semantics.
- Bound in-flight message count and bytes.
- Validate or explicitly provision the `v1alpha1` managed Queue topology.
- Verify fairness, redelivery, broker restart, TCP delay/disconnect, Linux race safety and server-managed topology compatibility.
- Replace sequential empty-priority polling with bounded continuous prefetch, sustaining the qualified eight-level workload.
- Treat connection loss, server shutdown and missing heartbeats as recoverable while preserving terminal consumer errors.
- Test locally against the server repository's pinned NATS `v2.14.1` baseline.

The SDK is a native NATS JetStream client. AMQP wire compatibility is outside the v0.1 scope.
