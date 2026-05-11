# 数据结构设计

## 数据结构文档

> 简单起见，使用Redis的基础数据结构作为标题。大部分操作还处于适配开发阶段。

### String

暂时通过 moka进行实现。包括下列数据结构的所有key-value操作，都使用 moka作为基座。

### Hash

通过 Rust自带的hashmap进行实现。如要读写并发，只能使用flurry。

### List

使用 `std::collections::LinkedList` 实现。读写并发可以用crossbeam SegQueue实现。

### Set

无序集合 暂用rust的hashset实现。如要读写并发，只能使用flurry。

### Sorted Set

在 Redis中也叫 Zset 。暂时用自带的 BTreeSet来进行实现 crossbeam的skiplist。

### 其他

已知Redis还支持bitmap，hyperloglog，Geospatial，Streams，Bitfield等。暂未有计划支持。

## 读写并发模型

在Redis中有一个核心的执行队列，dispatcher，所有的命令都通过单线程执行。但在raft中这是不合适的，读命令和写命令的延迟差距较大。读命令通常可以在租约期内直接返回，写命令则要经过一轮完整的共识算法。此外raft核心执行也是单线程的。
自然而然的，cache-cat使用读写并发的Hashmap。

Rust的moka库已经保证了每个单key操作是原子的。读读之间显然是不冲突的。写写之间是串行的，因此也不会产生原子性问题。（这里的写包括了删除操作）

|                 | 单key写（多次） | 多key写 | 单key读 | 多key读 |
| --------------- | --------------- | ------- | ------- | ------- |
| 单key读（多次） | √               | x       | √       | √       |
| 多key读         | x               | x       | √       | √       |

当多key读遇到了多次的单key的写。比如这里执行mget读k1和k2：读取k1->写入k1->写入k2->读取k2。导致原子性出现问题，多key写同理。

当单key读取遇到了多key写：写k1->读取k1->读取k2->写k2，这里就读取到了中间状态。

Cache-cat中使用read_lock和write_lock来优化这个问题。

|                 | read_lock（RwLock） | write_lock（Mutex） |
| --------------- | ------------------- | ------------------- |
| 单key读（多次） | 读锁                | x                   |
| 多key读         | 读锁                | √                   |
| 单key写（多次） | x                   | √                   |
| 多key写         | 写锁                | √                   |

由于多key的读写是绝对的少数操作，只有多key操作下才会需要上俩把锁。









