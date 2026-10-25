# BLPOP 与 Raft

`BLPOP key [key ...] timeout` 的等待状态仅保存在接入主节点本地。timeout 按 Redis 秒数解析，支持小数与 0 无限等待；有限等待使用固定的本地单调时钟 deadline，重试不重置。

## 确定性状态变更

Raft 日志中的 `RedisBlPop(BlPopReq)` 仅包含有序 keys 与 PopSide。`MultiReadComputeCommand` 按 key 顺序检查类型和列表内容，找到第一个非空列表后克隆并弹出，返回 `[key, element]`，保留原 TTL；最后一个元素被弹出时删除 key。没有数据时返回 null array，不产生写入。后续 key 不影响已经找到的首个可用列表。

快照增量记录计算后的 Insert/Del，恢复时不重新选择 key。waiter、deadline、连接信息均不进入日志或快照。PopSide 和等待注册机制可以复用于后续 BRPOP 等命令。

## 本地等待与通知

处理器先进行 lease read，然后在 write_lock + read_lock 内检查并注册，避免丢失生产者通知。无排队者且已有数据时只检查一次、提交一次。空列表路径等待事件，唤醒后才提交确定性 Raft 操作；竞争导致空结果时重新检查并注册。

列表生产者仅记录 touched key，在最外层命令结束后检查最终状态并发出纯信号。EXEC、Lua 内的操作不提前唤醒。通知回调不修改数据，也不等待 Raft 提交。BLPOP 自身检查所有请求 key 的剩余可用性，以覆盖“由 B 唤醒、实际从更早的 A 弹出”的情况。

列表使用专用 BlockingKeys registry。多个客户端可同时等待，但已经唤醒的阻塞 pop 在 Raft 完成前占有一个本地提交名额；后续 readiness hints 合并保存。这个名额在列表 registry 全局串行，包括不同数据库，保证重叠 key 的 FIFO，并避免一次 PUSH 唤醒所有客户端。普通命令及无需排队的 BLPOP 不受该名额约束。成功后依剩余数据继续唤醒下一位，取消则将尚未消费的提示交接给下一位。

## 事务与生命周期

MULTI 中只入队，EXEC 按非阻塞 Raft 操作执行。等待处理器复用 XREAD 的输入缓冲与断连检测，保存流水线输入供后续命令执行。Raft server_metrics 的角色/任期变更事件取消旧主上的 waiter；该订阅不随每条日志唤醒。角色转移返回 READONLY，客户端通过 Sentinel 重试，等待状态不迁移。

正常 timeout 返回 null array。已经提交的 Raft 写不由本地 timeout 取消，以免向客户端错误报告“未消费”。断连或节点关闭清理本地注册；若写已经提交，其最终结果仍由 Raft 决定，所以中断返回错误而非 timeout null。

## 性能边界

普通 GET 路径没有改动。普通非列表写入不访问列表等待 registry、ready_lists 或通知标志。LPUSH/RPUSH 与可能写入列表的 Insert/RENAME 等路径，在没有 waiter 时通过原子读取跳过 key 克隆、哈希和通知锁。额外的多 key 检查、注册、FIFO 调度和重试仅属于阻塞列表命令。
