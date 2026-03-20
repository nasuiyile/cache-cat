# 快照逻辑

## 常见的快照策略

Consul，使用go-memdb进行存储，其实现了内存mvcc机制。zookeeper，将所有操作变成CAS操作，因此后续的请求可以被应用多次，从而达到相同的数据逻辑。

Dragonflydb，通过任期手动标识哪些数据是旧的哪些数据是新的，执行完快照逻辑后手动收集变动部分数据。ETCD，通过存储层的数据库（BoltDB）来直接实现MVCC快照。Redis
通过linux下的fork指令（linux的fork会复制整个进程的地址空间，但只复制调用fork的那个线程）。

然而以上数据结构都存在特定的问题。Consul，Zookeeper，ETCD 只支持非常单一且特定的数据结构。
Kvrocks，pikiwidb，Tendis均采用Rocksdb，使用Rocksdb的MVCC机制来实现快照隔离。
而Redis的fork存在问题，当快照期间遇到大规模的数据写入时，fork操作会产生大量的内存占用。这是因为fork虽然是COW的，但是每一个值的修改，都会导致完整的内存页进行一次复制（在x64系统下通常是4kb）。并且只要父进程持续写入，就会不停的发生页复制，如果磁盘性能不够强会导致snapshot花费很久时间加剧了页复制的情况。
此外fork操作还会造成一个短暂的STW停顿，主进程在内核中执行fork的一段时间无法处理对外的客户端请求。生产上为了减小fork的影响，会限制Redis的容量上限。

Dragonflydb避免了fork操作。https://www.dragonflydb.io/docs/managing-dragonfly/snapshotting
。但引入了新的问题，https://github.com/dragonflydb/dragonfly/issues/6578 。
Dragonflydb为hashmap定制了一个通用的快照策略，因此能兼容Redis的所有数据结构。然而，当map中的一个key的value是一个较大的值时，由于读写操作不能同时访问该key，从而导致对该键值对进行快照会消耗较长的时间。
试想你将Dragonflydb的List结构用作消息队列。当某个key的value有几十G时，此时进行快照会长时间对该键值对进行占用，写入操作只能在原地等待直到对于该键值对的快照完成。这是由于Dragonflydb目前没有为所有的数据结构提供专属的快照策略导致的。
仅针对于Hashmap的读写并发结构的设计是相对简单的。但当Hashmap的value为各种不同的如链表，Skiplist等数据结构时，设计的复杂度会变得非常高。

## cache-cat的前快照策略

> 前快照策略即构建完快照后最终产生的是快照开始数据点的数据快照。对应Dragonfly的conservative策略。

要在不停机的情况下实现快照是复杂的。因为raft必须要求快照是一个时间点的统一快照。并且cache-cat在内存中并没有维护mvcc策略。
以下是cache-cat实现快照的说明。
由于raft的状态机是单线程的。快照线程理论上可以并发，但由于通常快照的瓶颈在于磁盘，因此这里只采用一个线程来实现。
此外业务处理线程只会处理修改操作，而不处理读请求。

***

此外，我们将业务map称为cache_map，快照产生的临时map存放老数据称为old_map，此外还有个removed_map用于存放被删除的数据。请求都是一批一批来，一次append操作可能产生上百个key的改动。也可能是一个key的改动。

每一个value保存一个快照编号。以上俩个map均采用moka实现（moka自带了机制实现相同key的：读读，读写并发。）

该快照字段不会被持久化。如进行反序列化默认是0。

除此之外我们还要记录last_applied_log_id（快照产生时最后应用的日志id），last_membership（最后的成员配置，raft自用不用管），snapshot_num（快照编号
u32），snapshot_state（当前快照状态）。这些数据都被一个锁保护统称为meta_data，并且被一个parking_lot::mutex（待讨论）保护起来。

**快照线程**

获取meta_data的锁，如果获取到了就将快照编号自增，并将快照标记为开始，并且保存meta_data元数据。释放锁。

迭代读取cache_map所有数据，如果发现读取到了新版本的数据，那么就访问old_map，来获取老的值，如果读取不到就不写入当前值（老值不存在）。

结束时获取meta_data锁，设置快照完成。释放锁，将数据刷盘面，然后清空old_map和removed_map。

**业务处理线程**：所有的

1. 当有一批新的数据到来。通过锁获取meta_data。（一批数据只获取一次锁，等整批数据处理完了然后释放）
2. 更新last_applied_log_id
3. 如果当前snapshot状态为false。直接将数据写入cache_map结束。
4. 如果当前snapshot状态为true：
    - 当前操作为put：查询现有数据是否存在于cache_map，且是否比当前快照编号小。如果存在且小（代表这个数据是旧的），放到old_map中。然后原地更新到cache_map的值，设置的snapshot_num必须是当前版本的。
    - 当前操作为remove：查询现有数据是否存在于cache_map，且是否比当前快照编号小。如果存在且小（代表数据是旧的），放到removed_map中。然后原地删除cache_map的值。
5. 最后释放锁

存在的问题：rust中没有高效的并发Map可以作为old_map和removed_map的实现。Dashmap实现锁粒度非常粗，且读写操作都会产生阻塞。flurry和papaya都需要绑定一个guard，导致在tokio中的操作较为麻烦（不能配合await使用），因为guard不能切换线程。只能用moka或者自己进行实现。
此外该逻辑在每次写入的时候都会带来额外的复杂度。
当遇到复杂的数据结构时，每次需要在写入时先序列化，之后才能处理写入操作，当写入的操作写一个大key（比如一个list有上亿条数据）时，性能会下降。
## 后快照策略

> 总思想类似Zookeeper，将所有操作变成一个CAS操作。对应Dragonfly中的Relaxed快照策略。

我们依然要记录last_applied_log_id（快照产生时最后应用的日志id），last_membership（最后的成员配置，raft自用不用管），snapshot_num（快照编号
u32），snapshot_state（当前快照状态）。这些数据都被一个锁保护统称为meta_data，并且被一个parking_lot::mutex（待讨论）保护起来。

此外每一个value都会在内部维护一个版本号。
**业务线程**

1. 当有一批新的数据到来。通过锁获取meta_data。
2. 更新last_applied_log_id
3. 如果当前snapshot状态为false。直接将数据写入cache_map结束。
4. 如果当前snapshot状态为true：获取原来数据的版本号，如果没有旧的版本号，则版本被视为0。将版本号加1，将数据写入cache_map。最后将老版本号和当前操作写入磁盘。（将所有写入操作变为一个CAS操作）

**快照线程**

获取meta_data的锁，如果获取到了就将快照编号自增，并将快照标记为开始，并且保存meta_data元数据。释放锁。

进行快照操作迭代所有数据，将数据写入磁盘中。

获取meta_data锁，将快照标记为结束。理论上这里获取过锁，因此不会出现close-and-drain race问题。读取删除队列中的全部数据，写入磁盘。

**恢复操作**
读取全量数据，恢复到状态机。
读取增量数据，对每一条增量数据进行CAS操作。

存在的问题：
对于特殊的数据结构还是存在问题。比如链表。
操作：A->B
实际的备份状态：B

此外需要反序列化每条操作，对每条操作进行定制的指令。












