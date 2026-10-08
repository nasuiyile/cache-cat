# Bug 检查记录（2026-10-04）

本轮已阅读 `docs` 全部设计文档，检查协议命令、Lua/事务、Raft 状态机、快照及逻辑时钟过期路径。以下区分已修复的小范围错误和未修改的较大问题。未进行真实三节点故障注入、断电测试或完整 Redis 差分测试，不能据此认定其余路径没有问题。

## 已修复

| 问题 | 修复及验证范围 |
| --- | --- |
| `PSUBSCRIBE` 模式下 `PING` 返回格式错误 | RESP2 返回两个 bulk string：`pong` 和消息（无参数时为空串）；RESP3 返回普通 PING 响应。连接测试覆盖两种协议、两种首次订阅方式和带/不带消息。 |
| `BITFIELD SET` 错误受 `OVERFLOW SAT/FAIL` 影响 | SET 直接写入低位，OVERFLOW 只控制后续 INCRBY。覆盖有符号/无符号字段、三种溢出模式、已有/新建键及 TTL 保留。修复前回归用例已失败。 |
| `LREM` 交替匹配时最坏 O(N²) | 消除循环内逐个 `VecDeque::remove`，改为一次压缩对应前缀/后缀，零 count 使用 retain。最坏 O(N)，额外空间 O(1)；保留少量近端删除的局部开销。覆盖正负 count、顺序、环形列表及整数边界。 |
| 开启密码后 `HELLO 3 AUTH default password` 被提前 NOAUTH 拒绝 | 分发允许 HELLO 进入自己的认证逻辑，校验用户名和密码；缺少或错误凭证时不修改协议和连接名称。测试同时验证普通未认证请求仍被拒绝。 |

这些修改没有向所有数据命令加入扫描、排序或额外锁。

## 优先处理的一致性和可用性问题

### 1. P1：删除重建复用版本号，快照恢复可能改变数据

位置：`cache_cat/src/raft/types/core/mocha/cas.rs:135`、`:209`；`cache_cat/src/protocol/key/persist.rs:107`。

不存在的键每次创建都赋版本 1，CAS 回放仅比较版本号，无法区分同一个键删除前后的两代数据。

可复现的合法时序：

1. 快照前执行 `SET k 0 PX 100000`，键为 v1。
2. Start 阶段依次执行 `INCR k`、`PERSIST k`、`DEL k`、`SET k 10`，保存的增量版本为 `[2, 3, 4, 1]`。
3. 全量遍历在最后一次 SET 后捕获 `k=10, v1`。
4. 恢复时旧 INCR 的 CAS2 错误匹配新的 v1，变成 `11, v2`。
5. PERSIST 发现新键没有 TTL，返回不修改，版本保持 v2；后续 DEL 和 SET 均因版本不匹配被跳过。

最终应该是 **10**，实际成为 **11**。本轮已运行独立复现，使用真实全量快照序列化/加载及序列化后的 CAS 增量回放，实际断言结果确认为 11。修复需要跨删除/过期保留世代信息，或重新设计回放判定，不能只调整某个命令的版本递增。

### 2. P1：Lua 使用随机或无序读结果写入，副本永久分叉（协议读取已修复）

位置：`cache_cat/src/raft/types/core/mocha/request_handler.rs:198`、`cache_cat/src/protocol/lua_env.rs:144`、`cache_cat/src/protocol/set/srandmember.rs:74`、`cache_cat/src/protocol/set/smembers.rs:46`。

Raft 复制原始 EVAL，各节点独立重新执行。Lua 可调用使用本地 `thread_rng` 的 SRANDMEMBER，并将结果 SET；SMEMBERS/HKEYS/HGETALL 等本地 HashSet/HashMap 顺序也可驱动写入。

示例：

```redis
SADD bag a b c d e f g h
EVAL "local t={} for i=1,32 do t[i]=redis.call('SRANDMEMBER',KEYS[1]) end return redis.call('SET',KEYS[2],table.concat(t,','))" 2 bag chosen
```

即便不用随机命令，以下脚本也可能分叉：

```redis
EVAL "return redis.call('SET',KEYS[2],table.concat(redis.call('SMEMBERS',KEYS[1]),','))" 2 bag chosen
```

初次审查的独立复现让两个 MyCache 应用完全相同的序列化日志，比较实际保存值。两种脚本均观察到 `stored values differ: true`。

2026-10-08 按确定性读取方案修复：

- `ReadCommand` 和 `MultiReadCommand` 的 `execute_with_clock` 用于 Lua/EXEC 中读取；时钟来自已复制的 Raft 日志。TTL/PTTL 保留已有逻辑时钟处理。
- `SRANDMEMBER` 先按原始字节排序候选成员，再以 key 和日志时钟初始化固定 SplitMix64 算法；复用 SPOP 的既有种子与算法，保持 SPOP 输出序列。正 count 不重复、负 count 可重复，缺失键和错误响应类型不变。同一时钟、同一键和集合的重复读取会得到相同样本。
- `SMEMBERS`、`SINTER`、`SUNION`、`SDIFF`、`KEYS` 按原始字节排序；`HGETALL`、`HKEYS`、`HVALS` 按字段排序，保留字段和值的对应关系及 RESP2/3 类型。
- Lua/EXEC 中的 `DBSIZE` 按日志时钟统计存活键，消除后台过期回收进度差异。该路径需要 O(N) 扫描；普通 DBSIZE 仍为 O(1)。普通网络读取保持原来的随机/无序快速路径，其他命令没有新增扫描或排序。
- 新增相同序列化 EVAL/EXEC 日志的跨副本回归，覆盖不同哈希种子、插入顺序、容量、序列化重建容器、本地读时钟差异、二进制数据和实际 SET 结果；另有过期物理回收进度不同的 DBSIZE 回归。

范围限制：本次修复 Redis 命令读取产生的随机性和无序性，不代表任意 Lua 脚本已完全确定。Lua 自身的 `math.random`、地址相关行为及可变全局状态/快照恢复等仍需单独设计和验证。

本次验证：`cargo test -p cache_cat --lib --offline -- --skip test::tests::test_add` **274 通过、0 失败**，排除 1 项需要外部服务的测试；新增 13 项回归全部通过。修改过的 Rust 文件通过 `rustfmt --check`，`git diff --check` 通过。日志：`target/deterministic-read-tests.log`。

### 3. P1：合法 XREADGROUP 可触发状态机 panic，重放仍会失败

位置：`cache_cat/src/protocol/stream/xreadgroup.rs:157`、`:161`；`cache_cat/src/raft/types/core/mocha/request_handler.rs:59`。

命令已注册，但 MultiReadCommand 的 `keys()`、`execute()` 仍为 `todo!()`。

- 普通 `XREADGROUP GROUP g c STREAMS s >` 在连接任务中 panic、断开连接。
- `MULTI` → 相同 XREADGROUP → `EXEC` 会将操作写进 Raft 日志，在状态机 apply 中触发 panic；各副本及重启后的日志回放都会遇到该操作。
- Lua 工厂未注册 XREADGROUP，Lua 路径返回 UnknownCommand，不属于这里的 panic 路径。

这不能只通过填充一个读函数修复：消费组读取会修改 pending、消费者及投递游标，需要写操作、Raft 复制和快照支持。当前结论来自完整调用链静态追踪，未向运行中的集群发送此命令。

## 未修改：过期回收和其他协议问题

### 4. P2：持续写流量可能让过期回收无限推迟

位置：`cache_cat/src/node/raft_builder.rs:57`、`cache_cat/src/raft/types/core/mocha/core.rs:91`、`cache_cat/src/mocha/mod.rs:487`。

定时清理发现近期写时钟推进就直接跳过；更新逻辑时钟本身不唤醒过期 worker，而 worker 只等待消息，没有周期 tick。复现条件：先写一个短 TTL 大对象，随后持续写另一个永久键，不访问旧键、也不向其 DB 投递新 TTL。旧键虽逻辑过期，仍可能一直占内存。热 DB 的持续写入也会使冷 DB 的清理被跳过。

这超过设计文档容忍的短暂时间偏差。需要让 worker 基于已提交写时钟定期推进，同时避免为每条命令向所有数据库广播。

### 5. P2：保留 TTL 的热点写入重复积累定时器

位置：`cache_cat/src/mocha/mod.rs:282`、`:289`、`:550`、`:111`。

`SET hot 0 EX 86400` 后反复 INCR，每次写入保留同一过期时间，但都会新建一个 TimerItem，时间轮只追加，没有去重或取消；DEL/clear 也不主动移除这些记录。即使只有一个存活键，定时器数量仍随写入次数增长，通常要等到过期并推进时间轮才释放。

这是静态数据流确认的问题，未做长期内存压测。建议结合上一项设计定时器去重、取消与推进机制。

### 6. P2：XADD 自动 ID 在已有未来 ID 时错误失败

位置：`cache_cat/src/raft/types/core/mocha/request_handler.rs:91`、`cache_cat/src/raft/types/core/structure/stream/stream.rs:310`。

```redis
XADD s 999999999999999-0 f v
XADD s * f v
```

第二条应返回 `999999999999999-1`，当前会报 IdNotIncreasing。分发层把 `*` 转为 `AutoSequence(write_clock)`，混淆了自动 ID 和用户显式指定 `<ms>-*` 的语义；前者应在时钟落后时递增已有 ID，后者应拒绝落后的 ms。修复应保留这两种意图，并验证初始化、普通写入和快照回放的一致性。

### 7. P2：EVAL 不填充 EVALSHA/SCRIPT EXISTS 使用的脚本缓存

位置：`cache_cat/src/protocol/lua_env.rs:252`、`cache_cat/src/protocol/lua/evalsha.rs:152`、`cache_cat/src/protocol/lua/script.rs:270`。

```redis
SCRIPT FLUSH
EVAL "return 1" 0
EVALSHA e0e1f9fabfc9d4800c877a703b823ac0578ff8db 0
```

最后一条应返回 1，当前查不到脚本并返回 NOSCRIPT。EVAL 只填充编译函数 LRU，EVALSHA/SCRIPT EXISTS 查询的 script_map 只由 SCRIPT LOAD 填充。需要统一脚本缓存语义并验证 EXEC、FLUSH 和副本上的行为。

### 8. P2：部分命令整数解析仍接受 Redis 拒绝的形式

位置示例：`cache_cat/src/protocol/list/lset.rs:52`、`cache_cat/src/protocol/set/spop.rs:73`、`cache_cat/src/raft/types/core/response_value.rs:663`。

例如已有列表上的 `LSET list 01 value`、集合上的 `SPOP set +1` 会接受参数并修改数据，Redis 的整数转换会拒绝前导零、加号、负零等非规范形式。项目已有严格解析助手，但调用点尚未统一。涉及多个命令，建议逐类迁移并做参数错误与数据不变的差分验证。

## 需要进一步验证的持久化风险

`cache_cat/src/raft/store/snapshot/snapshot_handler.rs:104` 与 `cache_cat/src/raft/store/statemachine.rs:274` 发布快照时使用 rename，未同步父目录。在 POSIX 文件系统上，文件 sync_all 不等于 rename 后目录项已持久化；若覆盖日志已被压缩，突然断电可能丢失最新快照入口。该项仅为静态审查风险，未做 Linux 文件系统/断电复现，不能当作已经观察到的数据丢失。建议单独审查原子发布及日志压缩的持久化顺序。

## 验证记录

- 初始 `cargo test -p cache_cat --lib --offline`：249 通过，1 失败；失败的 `test::tests::test_add` 要求已运行的 `127.0.0.1:5001` 服务，本机没有该服务。
- `BITFIELD SET` 新回归在修复前已稳定失败，SAT 把应写入的 44 错写成 255。
- 两个核心复现已运行：快照得到错误值 11；两种 Lua 脚本均产生不同副本状态。输出保存在 `target/bug-review-reproductions.log`，诊断源码保存在 `target/bug-review-reproducers/`，临时测试入口已移除。
- 最终执行 `cargo test -p cache_cat --lib --offline -- --skip test::tests::test_add`：**253 通过，0 失败，1 项外部服务依赖测试排除**。日志保存在 `target/bug-review-tests.log`。
- 六个修改过的 Rust 文件均通过 `rustfmt --check`，`git diff --check` 通过。未开展吞吐量基准测试，LREM 的性能结论来自消除嵌套搬移的复杂度分析。
