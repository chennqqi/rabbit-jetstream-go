# 变更日志

[English](CHANGELOG.md) | [简体中文](CHANGELOG.zh-CN.md)

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
