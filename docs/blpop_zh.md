# BLPOP 与 Raft

公共执行模型、XREAD/XREADGROUP 的区别、超时边界及主从时序图见 [读取、阻塞与状态变更命令](blocking_reads_and_raft_zh.md)。

`BLPOP key [key ...] timeout` 的等待状态仅保存在接入主节点本地。timeout 按 Redis 秒数解析，支持小数与 0 无限等待；有限等待使用固定的本地单调时钟 deadline，重试不重置。

## 确定性状态变更

Raft 日志中的 `RedisBlPop(BlPopReq)` 仅包含有序 keys 与 PopSide。`MultiReadComputeCommand` 按 key 顺序检查类型和列表内容，找到第一个非空列表后克隆并弹出，返回 `[key, element]`，保留原 TTL；最后一个元素被弹出时删除 key。没有数据时返回 null array，不产生写入。后续 key 不影响已经找到的首个可用列表。

快照增量记录计算后的 Insert/Del，恢复时不重新选择 key。waiter、deadline、连接信息均不进入日志或快照。PopSide 和等待注册机制可以复用于后续 BRPOP 等命令。

## 本地等待与通知

处理器先进行 lease read，然后在 write_lock + read_lock 内检查并注册，避免丢失生产者通知。无排队者且已有数据时只检查一次、提交一次。空列表路径等待事件，唤醒后才提交确定性 Raft 操作；竞争导致空结果时重新检查并注册。

列表生产者仅记录 touched key，在最外层命令结束后检查最终状态并发出纯信号。EXEC、Lua 内的操作不提前唤醒。通知回调不修改数据，也不等待 Raft 提交。BLPOP 自身检查所有请求 key 的剩余可用性，以覆盖“由 B 唤醒、实际从更早的 A 弹出”的情况。

流和列表的 touched key 都收集到 `ApplyEffects`，由串行 apply 独占，并通过 `Update.effects` 的可变借用传给嵌套命令。这些临时集合不再放在 `MyCache` 中，不需要独立 Mutex 或 pending 原子标志。每批 apply 创建一份上下文，小集合复用容量，超过 64 容量的集合在排空后释放，避免大事务让后续小通知扫描大表。每条最外层命令结束时排空；包括 Lua 写入后返回错误的情况。记录保留实际 `(db, key)`，因此跨数据库 EXEC 不会误用最后选中的数据库。快照 CAS 恢复使用 `effects: None`，不产生本地通知。

全局写锁保护 `WriteState`，流和列表的两个 `BlockingKeys` 注册表及其注册入口都只保存在这里，`MyCache` 不再保存另一套同名句柄。注册表内部的普通可变状态不再有独立 Mutex 或原子 waiter 计数；apply 通过 `Update.waiters` 借用已有 guard。注册后返回的 `Registration` 仅持有接收端和回访同一全局写锁所需的弱引用等信息，不拥有另一份注册表。多个客户端可同时等待，但已经唤醒的阻塞 pop 在 Raft 完成前占有一个本地提交名额；后续 readiness hints 合并保存。这个名额在列表 registry 全局串行，包括不同数据库，保证重叠 key 的 FIFO，并避免一次 PUSH 唤醒所有客户端。普通命令及无需排队的 BLPOP 不受该名额约束。成功后依剩余数据继续唤醒下一位，取消则将尚未消费的提示交接给下一位。

## 事务与生命周期

MULTI 中只入队，EXEC 按非阻塞 Raft 操作执行。等待处理器复用 XREAD 的输入缓冲与断连检测，保存流水线输入供后续命令执行。Raft server_metrics 的角色/任期变更事件取消旧主上的 waiter；该订阅不随每条日志唤醒。角色转移返回 READONLY，客户端通过 Sentinel 重试，等待状态不迁移。

正常 timeout 返回 null array。已经提交的 Raft 写不由本地 timeout 取消，以免向客户端错误报告“未消费”。断连或节点关闭清理本地注册；若写已经提交，其最终结果仍由 Raft 决定，所以中断返回错误而非 timeout null。

注册与数据检查在同一全局写锁内完成；等待信号或 Raft 提交前释放锁。正常 timeout、cancel 和 finish 获取同一写锁完成状态转换。timeout 必须在锁内仲裁：仍在等待队列才超时，已被选中的响应或弹出机会继续完成。finish 在等待锁前标记已经消费，避免此时被取消后错误重发提示。

`Drop` 不能异步获取写锁，因此每个未完成注册至多投递一条清理事件，由首个有效阻塞注册惰性启动的单一任务处理；同步创建缓存不要求 Tokio runtime。清理任务通过弱引用访问写锁，每批最多处理 64 条事件后释放锁并让出执行机会，空闲时等待事件，不扫描注册表或轮询。通道不额外限制客户端数，其内存随尚待清理的注册数增长；不会为每次断连创建一个任务。

## 性能边界

普通 GET 路径没有改动。全局 `read_lock`、`write_lock` 及 apply 按批次获取写锁的范围保持不变。普通写入在命令结束时仅检查本地集合是否为空，不再无条件获取流通知队列的锁；空集合不访问等待 registry，也不进行原子操作、哈希或堆分配。LPUSH/RPUSH、列表 Insert/RENAME 和 XADD 仅在已有写锁的 guard 内检查普通 waiter 状态；无等待者时不克隆 key、不哈希，也不再读取原子 waiter 计数或获取注册表锁。正常生产者路径不访问清理通道；只有等待句柄析构的后备清理使用它。额外的多 key 检查、注册、FIFO 调度和重试仅属于阻塞列表命令。
