# Snapshot Logic

## Common Snapshot Strategies

Consul uses go-memdb for storage, which implements an in-memory MVCC mechanism. Zookeeper converts all operations into CAS operations, so subsequent requests can be applied multiple times to achieve the same data logic.

Dragonflydb manually marks which data is old and which is new via terms, and after executing the snapshot logic, manually collects the changed data. ETCD uses a storage-layer database (BoltDB) to directly implement MVCC snapshots. Redis uses the fork instruction under Linux (Linux's fork copies the entire address space of the process, but only copies the thread that called fork).

However, all of the above data structures have specific issues. Consul, Zookeeper, and ETCD only support very simple and specific data structures. Kvrocks, pikiwidb, and Tendis all use Rocksdb, directly leveraging Rocksdb's MVCC mechanism for snapshot isolation.

Redis's fork approach has problems: when large-scale data writes occur during a snapshot, the fork operation generates significant memory usage. This is because although fork uses Copy-On-Write (COW), every modification to a value causes a full memory page to be copied (typically 4KB on x64 systems). Moreover, as long as the parent process continues writing, page copying continues, and if disk performance is insufficient, the snapshot can take a long time, exacerbating page copying. Additionally, the fork operation causes a brief STW pause, during which the main process, while executing the fork in the kernel, cannot handle external client requests. In production, to mitigate the impact of fork, Redis's capacity is often limited.

Dragonflydb avoids the fork operation. (https://www.dragonflydb.io/docs/managing-dragonfly/snapshotting) However, it introduces new issues. (https://github.com/dragonflydb/dragonfly/issues/6578) Dragonflydb has customized a general-purpose snapshot strategy for its hashmap, making it compatible with all Redis data structures. However, when the value of a key in the map is large, since read and write operations cannot access the same key simultaneously, snapshotting that key-value pair can take a long time.

Dragonflydb internally uses a fiber, and this fiber only yields when it voluntarily gives up the CPU or when a task blocks. (Both fork and the main thread are actually single threads.) Imagine using Dragonflydb's List structure as a message queue. If a certain key has a value of several tens of GB, performing a snapshot will occupy that key-value pair for an extended period, and write operations will have to wait until the snapshot for that pair is complete. This is because Dragonflydb currently does not provide dedicated snapshot strategies for all data structures.

Designing read-write concurrent structures solely for Hashmap is relatively simple. However, when the value in a Hashmap can be various data structures such as linked lists, skiplists, etc., the design complexity becomes very high.

Additionally, Dragonflydb's strategy causes the thread that is writing during the snapshot to also serialize data (which increases latency). Furthermore, each serialization operation serializes the entire value under the key. Therefore, Dragonflydb cannot store large-sized data in values.

Without MVCC and without using the fork instruction, only Dragonflydb's strategy and Zookeeper's strategy can be referenced. Below is a comparison of the drawbacks of both. By default, Zookeeper makes every instruction a CAS operation; here, we assume that version numbers are only recorded during the snapshot period.

Drawbacks of Dragonflydb:

- When the snapshot thread has not yet serialized the data, every write operation that occurs will first cause blocking until serialization is completed and sent to the persistence thread. This introduces potential latency increases.
- Since data is uniformly serialized first and then sent to the snapshot thread, additional memory is consumed (and theoretically, excessively large values cannot be stored, e.g., when a single List contains tens of GB of values). Even though this memory is released after writing is complete. Zookeeper can write to disk while traversing.

Drawbacks of the Zookeeper approach (the solution below):

- All version data must exist in the snapshot. (Both approaches require maintaining a u32 version number in memory, but Dragonfly's approach theoretically does not need to write this version number to disk.)
- During snapshot installation, each write instruction must implement a corresponding CAS operation. This greatly increases the complexity of adaptation.
- Incompatible with Redis snapshot format.
- During snapshot building, all data must be cached. At this point, all requests result in doubled memory usage. (Zookeeper's own consensus algorithm can avoid this issue.)

## Snapshot Strategy

> The following is Cache-cat's own snapshot strategy; the overall idea is similar to Zookeeper — converting all operations into CAS operations. It corresponds to Dragonflydb's Relaxed snapshot strategy. Zookeeper's approach has a drawback: Raft's snapshot semantics require the snapshot to be an instantaneous snapshot at a single point in time.

Each value in the map can be treated as an atomic access unit. Any operation on this value is atomic and supports concurrent reads and writes.

We still need to record `last_applied_log_id` (the last applied log ID at the time the snapshot was generated), `last_membership` (the last membership configuration, internal to Raft, no need to worry about), `snapshot_num` (snapshot number, u32), `snapshot_state` (current snapshot state). This data is collectively referred to as `meta_data` and is protected by a single lock (parking_lot::mutex, to be discussed).

Additionally, each value maintains a version number internally (u32 type).

**Business Thread**

1. When a new batch of operations arrives, acquire the meta_data lock.
2. Update `last_applied_log_id`.
3. If the current snapshot state is `End`, write the data directly to cache_map.
4. If the current snapshot state is `Start`: get the version number of the original data (treated as 0 if there is no old version). Increment the version number by 1 and write it to cache_map. Finally, push the old version number and the current operation onto a queue for temporary storage. (This turns every write operation into a CAS operation.)
5. If the current snapshot state is `Tail`, apply the Raft log through the normal path without writing it to the snapshot's incremental queue. This log sits after the snapshot's recorded `last_applied_log_id`, and is replayed from the Raft log during recovery.

**Snapshot Thread**

Acquire the meta_data lock, mark the snapshot state as `Start`, then release the lock. This records the snapshot state, but the final `last_applied_log_id` is only determined once the state enters `Tail`.

Perform the snapshot operation: iterate over all data and write it to disk.

The snapshot state has three phases:

- `Start`: the snapshot thread is iterating over the full dataset. Writes applied by the business thread must update memory at the same time, and push the operation — tagged with the old version number, database number, and write logical clock — onto the incremental queue.
- `Tail`: the full dataset traversal is complete. The snapshot thread acquires the `meta_data` lock, and under that lock switches the state to `Tail`, atomically drains and clears the incremental queue accumulated during the `Start` phase, and records the `last_applied_log_id` at this moment. From the switch to `Tail` onward, new Raft logs no longer go into the snapshot's incremental queue and are instead applied through the normal path; these logs are numbered after the snapshot's recorded `last_applied_log_id`, and are replayed from the Raft log during recovery.
- `End`: once the incremental queue has been written to the snapshot file, and the file has been flushed to disk and has replaced the current snapshot file, the state switches to `End`.

Draining the queue and switching the state during the `Tail` phase must happen under the same `meta_data` lock, to avoid an operation being incorrectly added to the snapshot's queue after it has already been drained. Raft logs produced during the `Tail` phase can proceed concurrently with flushing the snapshot file to disk, but must not be treated as part of the snapshot's incremental queue.

**Recovery Operation**

Read the full snapshot data and restore it to the state machine. (Pause external access during this time.)

Read the incremental data from the queue and perform a CAS operation on each item. Apply the operation only if the current data's version number matches the version number recorded in the queue.

The `last_applied_log_id` recorded in the snapshot is the log position at the moment the state entered `Tail`. After installing the snapshot, first replay the incremental queue saved during the `Start` phase in CAS order; Raft then continues applying the log from that position onward, corresponding to the writes from the `Tail` phase and afterward.

Why is this correct? The snapshot thread and the business thread run concurrently. Therefore, when the snapshot thread is executing the snapshot, two types of data are preserved — old and new (each data item is atomic).

When the snapshot starts, all operations executed by the business thread are stored in the queue. All queue elements saved are CAS operations with version numbers.

Therefore, when restoring a snapshot, simply restore the snapshot data + apply CAS operations on the queued data, and the final result will be the correct latest data.

The snapshot thread briefly locks the state machine at the start and end. The fully restored data is what was in memory at the time of the second lock. Therefore, this snapshot strategy is often considered a post-snapshot strategy.

Existing issues:

Each operation needs to be deserialized, and custom instructions must be provided for each operation.

## Multi-key Operations

The approach above still doesn't solve one problem: for commands like SINTERSTORE or SUNIONSTORE, we cannot guarantee that all the keys read in a batch are at the same version.

Suppose data A is at version v1 and B is at version v1.

1. The snapshot records B at v1.
2. You read A and B, and modify C (then push this onto the execution queue).
3. A subsequent write modifies A to v2.
4. The snapshot records A at v2.

If the snapshot were to simply replay your operation at this point, it would find that neither of the two versions the operation depends on is the version currently in effect.

Therefore, for this kind of command, the snapshot logic treats it as multiple commands; from Raft's perspective it remains a single command, to preserve the atomicity of the command. But for the snapshot, multiple commands need to be treated as a single command, to preserve the atomicity of the snapshot.

## Read-Write Logical Clock Compatibility

> Problem: When an operation is pushed to the queue, how do we know the result of the pair after the operation? If the primary node executes this operation, it could be before or after the key expires.

When the snapshot starts, every operation being performed is pushed to the operation queue, and the generated write logical clock is also pushed to the queue (this write logical clock is the deterministic result after computing `max(write_logical_clock, read_logical_clock, current_timestamp)`).

**Snapshot Recovery**

First, apply the full state machine. When the state machine is initialized, the write logical timestamp is 0. Therefore, no data is expired. Next, apply the incremental queue.

During recovery, first restore the write logical clock from the queue, then directly apply the operation sequence. The write logical clock is in the operation queue, and each operation is deterministic. When the first log in the queue is applied, the write logical timestamp will be updated. At this point, data begins to follow the normal expiration logic. After each operation is executed, which data should be expired is deterministic.

The order in which logs are applied is the same as on the primary node. The only difference is that if the version number does not match, the data will not actually be modified.

The recorded operation queue is ordered. Therefore, when restoring a snapshot, simply restore the snapshot data + apply CAS operations on the queue data, and the final result will be the correct, latest data.

Finally, when the snapshot is complete, the node's metadata and write logical timestamp are also saved under lock.
