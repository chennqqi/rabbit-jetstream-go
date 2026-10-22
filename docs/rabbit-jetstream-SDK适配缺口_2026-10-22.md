# rabbit-jetstream-go SDK 适配缺口反馈

日期：2026-10-22；对象：`github.com/chennqqi/rabbit-jetstream-go`（tag `v0.1.0-rc.1`，commit `b7acd52`）；评估方：aurivex `runtime/queue` 适配。目的：把 Port 契约无法 1:1 落位的原语缺口反馈给上游，供 SDK 下一版本补面。

## 背景：我们的 Port 契约

`runtime/queue` 的消费者协议（P02-01）要求适配器提供五个原语：

| 原语 | 语义 |
| --- | --- |
| 确认发布 | 发布在 broker 确认后才返回（at-least-once 的前提是发布侧先确证） |
| 投递身份 | 消息 ID 在重投全程稳定，用于幂等关联 |
| 逐投递计数 | `Delivery.Deliveries` = 该 ID 的第 N 次投递；超 `MaxDeliveries` 由适配器转隔离（`REDELIVERY_EXHAUSTED`），不无限重投 |
| 延迟 Nak | `Delivery.Nak(delay)` 在 delay 后重投——消费侧用它实现 `NakDelay×attempt` 有界退避（`runtime/consumer`）；立即重投会让瞬时故障（内容暂不可读、provider 限流）变成热重试循环 |
| 毒隔离 | `Quarantine(reason)` 带可审计原因单向隔离，绝不自动重投 |

## SDK 现状对照（v0.1.0-rc.1 源码逐行核对）

| 原语 | SDK 现状 | 结论 |
| --- | --- | --- |
| 确认发布 | `Publisher.Publish` 返回 `PublishAck{Stream, Sequence, Duplicate}`（PubAck 等待） | ✅ |
| 投递身份 | `PublishMessage.ID` → `Nats-Msg-Id` → `Message.ID`，契约头校验 | ✅ |
| 逐投递计数 | `Message` 不暴露 JetStream consumer metadata（`NumDelivered`）。SDK 内部 `raw jsapi.Msg` 私有，外层拿不到 | ❌ 缺口 1 |
| 延迟 Nak | `Message` 仅暴露 `Ack()`/`Nak()`（立即）/`Term()`/`InProgress()`；`jsapi.Msg.NakWithDelay(d)` 未透出 | ❌ 缺口 2 |
| 毒隔离 | `Term()` + 控制面 DLQ 可等价；或重发隔离队列后 Ack（同 Rabbit 适配器模式） | ✅ 有等价路径 |
| 断线重连 | nats.go 连接层原生自动重连 + pull 循环恢复 | ✅ 优于 AMQP 适配器 |
| 管理读面（深度/积压） | SDK 未暴露 StreamInfo/ConsumerInfo 查询；需调用方自持 `jsapi.JetStream` 旁挂 | ⚠ 缺口 3（可旁挂绕过） |

## 请求上游补面

1. **`Message.NakWithDelay(delay time.Duration) error`** —— 透传 `jsapi.Msg.NakWithDelay`。JetStream 原生按消息延迟重投，没有 RabbitMQ per-message-TTL 等待队列的队头阻塞问题，是我们想要的 settle 原语。替代方案（重发延迟队列+Term 原件）需要额外拓扑且崩溃窗口语义变差。
2. **`Message.Deliveries() int`**（或 `Metadata()`）—— 透传 `jsapi.Msg.Metadata().NumDelivered`。这样适配器可以用 broker 权威计数实现 `MaxDeliveries` 门，不必靠"重发时塞 header"这类应用层计数（header 计数在 broker 侧 Nak 重投路径下不递增，两种机制会互相打架）。
3. **`Consumer.Pending(ctx) (int, error)` 或等价的积压读面** —— 调度器过载拒绝（`MaxOutstanding`）需要 broker ready 深度作输入；目前只能旁挂 `jsapi` 查 `ConsumerInfo.NumPending`，能用但多开一个管理上下文。

可选（非阻断）：`InProgress` 已暴露，worker 长任务租约续期可直接用；`Publisher` 的 `policyRevision` 参数建议允许空值跳过契约头校验（standalone/测试拓扑下没有控制面下发的 plan revision，`EnsurePriorityQueue` 场景 revision 无意义，目前只能塞占位串）。

