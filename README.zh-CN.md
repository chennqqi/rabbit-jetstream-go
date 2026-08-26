# rabbit-jetstream-go

[English](README.md) | [简体中文](README.zh-CN.md)

`rabbit-jetstream` 服务端发行版的原生 Go SDK。它在不修改 NATS/JetStream 的前提下实现公开的 `v1alpha1` 契约。首版通过该 SDK 提供 RabbitMQ 风格优先级队列，不基于 AMQP；完整 RabbitMQ/AMQP 线协议兼容属于服务端未来 Roadmap。

v0.1 生产资格范围为 Linux/AMD64、三节点、三副本和最多八个优先级（`MaxPriority <= 7`）。更高优先级数量属于实验配置。

SDK 将消息发布到 `rjs.q.{queue}.p.{priority}` 并等待 JetStream PubAck。每个优先级 durable consumer 持续预取到有界缓冲区，`Next` 从高到低扫描；公平性调度会在高优先级 burst 后探测低优先级，避免永久饥饿。

```text
go test ./...
go test -race ./...
(cd test/fault && go test ./...)
./scripts/test-integration.sh
pwsh ./scripts/test-integration.ps1
go run ./cmd/rjs-sdk-bench --help
```

交付语义为 at-least-once。业务处理持久化后才能 Ack，并应保留稳定消息 ID 实现幂等。`ConsumerConfig.Prefetch` 是每个优先级的本地缓冲大小；默认总预算为 256。优雅退出时调用 `Consumer.Close`，使缓冲消息通过 NAK 尽快重投。
