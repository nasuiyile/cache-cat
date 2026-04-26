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
自然而然的，cache-cat使用读写并发的hashmap。

然而当前系统还需要保证每个操作对外的原子性。为了适配redis可能出现多键值操作。为了实现多键值对外的原子性。
首先给空对象加一个读写锁。读取和写入 时候正常执行。读取的时候上读锁，单键值写入不上写锁，多键值写入上写锁。
单key的时候让moka自己去保证操作的原子性，多key的时候就通过锁来保证 。此外写入操作是只有一个线程执行的，读取操作是多线程的。

理论上最好是通过request_id的模式来实现multiplexing（多路复用）。但是为了兼容redis的协议，推荐客户端用更大的连接数来避免raft更严重的队头阻塞问题。