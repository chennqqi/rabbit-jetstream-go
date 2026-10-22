# 变更日志

[English](CHANGELOG.md) | [简体中文](CHANGELOG.zh-CN.md)

## 未发布

- 新增 `Message.NakWithDelay`：由 broker 按消息延迟重投，无需延迟拓扑即可实现有界消费退避。
- 新增 `Message.Deliveries`：暴露 broker 权威投递次数，适配器可实现 `MaxDeliveries` 门控而无需应用层计数。
- 新增 `Message.TermWithReason`：为终止消息附带可审计原因（要求 NATS server >= 2.10.4）。
- 新增 `Consumer.Pending`：汇总所有已绑定优先级 consumer 的 broker 侧就绪积压。
- `Publisher` 的 policy revision 允许为空，用于 standalone 和 `EnsurePriorityQueue` 测试拓扑，此时省略 revision 契约头。

## v0.1.0-rc.1

- 使用稳定消息 ID 和同步 JetStream PubAck 发布优先级消息。
- 通过有界严格优先级调度消费每个优先级的 durable pull consumer。
- 提供 Ack、Nak、Term 和 InProgress，并采用 at-least-once 交付语义。
- 限制 in-flight 消息数量和字节数。
- 校验或显式创建 `v1alpha1` 受管 Queue topology。
- 验证公平性、重投、broker 重启、TCP 延迟/断连、Linux race 和服务端 topology 兼容性。
- 使用有界持续预取替代顺序空优先级轮询，满足八级优先级资格负载。
- 将断连、服务端关闭和心跳缺失作为可恢复错误，同时保留终止类 consumer 错误。

本 SDK 是原生 NATS JetStream 客户端；AMQP 线协议兼容不属于 v0.1 范围。
