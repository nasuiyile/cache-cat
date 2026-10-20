# Clock Design

## Problem Description

Cache-cat aims to implement a Raft-based version of Redis. Consider these two Redis commands:

```text
SET key value PX <fixed expiration time>
```

Destroy the value after the fixed expiration time.

```text
SET key value NX
```

Set the value when the key does not exist; do nothing when it already exists. Redis has many similar read-modify-write (RMW) commands; this is only an example.

If the command is executed on both primary and replica nodes, clock skew means that the key expires at different times on the two nodes.

The clocks on the primary and replica cannot be synchronized perfectly. If the primary has expired the key while the replica has not, a subsequent `SET NX` produces different results on the two nodes, leaving them permanently inconsistent.

Any clock operation can be viewed as a system call, and system calls introduce nondeterminism. A Raft state machine requires the same initial state on every node, with each iteration depending only on the previous iteration. Using wall-clock time would make the state-machine iterations depend on an uncertain external factor.

## Basic Assumptions and Background

> Clock offset (or skew) and clock drift are different concepts.
>
> **Clock offset:** the time difference between two clocks at a particular instant.
>
> **Clock drift:** the difference between a clock's running rate and ideal time; that is, whether it runs fast or slowly.

Cache-cat's overall instruction throughput exceeds 300,000 instructions per second using the Raft protocol. In the absence of failures, a log can normally be committed within milliseconds.

For timed expiration, a deviation of several tens of milliseconds is normally acceptable. Machines themselves accumulate clock error over long periods of operation.

Cache-cat already uses lease reads to optimize read performance, so this design assumes bounded clock drift. Lease reads are common in systems such as TiDB and etcd.

The external semantics must remain strongly consistent, as in the Raft design: clients see the behavior of one state machine. It must be impossible to set a key to expire, read it as expired, and then read it as present again.

Reads from replica nodes can provide only eventual consistency, so strong consistency for replica reads is outside this design's scope.

## Reference Solutions

> Because cache-cat uses Raft, each design below is adapted to a Raft state machine.

### Deterministic Instruction Approach

This approach creates atomicity problems and is therefore rejected.

First read the current state-machine state on the primary, generate a deterministic instruction, and submit that instruction. For example, convert `SET NX` into a deterministic `SET` instruction.

The state machine can change after the read because only applied Raft logs modify it. Two concurrent `SET NX` commands could therefore both succeed. This approach works only when the current operation is guaranteed to be the first operation committed after the read.

### Initiate Additional Consensus on Deletion

> etcd, ZooKeeper, and Consul use this idea for timed deletion. They are not cache systems and do not need deletion to happen at cache frequency.

Each timed deletion starts another round of Raft consensus. The primary sends the deletion to replicas, and the deletion completes only after consensus.

When a read encounters data that has already expired (Redis lazy deletion), cache-cat would have to reach consensus to delete the data before replying. This increases read latency.

> Redis does not have this problem because commands streamed from the primary to replicas do not require a response. Redis does not wait for replicas to finish before replying to the client; there is no backpressure mechanism. Raft `AppendEntries`, however, requires most nodes to persist and reply before it completes, which delays the client response.

The performance cost is substantial. Setting expiration is frequent in a cache. Although the deletion is initiated by the primary rather than by the client and therefore saves one round trip, it can nearly double the number of logs that Raft must commit and significantly reduce throughput.

One optimization is to batch multiple periodic deletions into a single submitted operation.

### Lamport Clock Approach

Since no absolute clock exists, treat the clock itself as a deterministic parameter. Every 10 ms, the primary submits its current local time as a log entry and uses it as logical time.

Timed and lazy deletion then use this logical time. Expiration no longer refers to an unstable local relative clock; it refers to the logical time established by consensus.

This removes the nondeterminism and prevents clock rollback.

The tradeoff is expiration error and continuous work while the machine is idle. An idle node would still submit 100 logs per second to disk.

When the primary is continuously submitting logs to replicas, those logs can also replace the primary's heartbeats. By default, Raft sends a heartbeat to replicas every 50 ms.

## Clock Consensus Scheme

This is the self-developed scheme ultimately adopted by cache-cat.

> Based on our research, infrastructure such as etcd, ZooKeeper, and Redis does not assume that the clock offset between primary and replica nodes is bounded.
>
> TiDB's PD cluster and CockroachDB make similar implicit assumptions because they use an HLC scheme.

This design still assumes a bounded clock offset between primary and replica nodes. Within that offset range, a new leader cannot be elected; for example, the offset may be less than one second.

> Leader-election time > clock-offset time

When a write operation requires a Raft log, the primary generates a write logical timestamp from its local time:

```text
max(write logical time, read logical time, physical time)
```

Every operation carries this timestamp. When a replica receives the log, it uses the timestamp to update its local logical-time state before processing the operation. All writes are therefore deterministic. Advancing logical time is attached to each entry, and the overhead is negligible: it changes one local integer.

**The expiration time of entries in the core cache map changes from physical time to write logical time.**

Both primary and replica nodes process writes using the logical timestamp. Lazy and timed deletion are also driven deterministically by the state machine's logical time. Write consistency follows from deterministic state-machine iteration over the Raft log.

Reads are the main optimization target. A read also generates a logical timestamp using:

```text
max(write logical time, read logical time, physical time)
```

Unlike write timestamps, read timestamps do not need consensus. They are maintained in memory and updated for each read. A node's read timestamp is monotonic. Because the clock offset is bounded and leader-election time is greater than that offset, the new primary's physical time is greater than the old primary's physical time after a leader change. Thus read logical time is globally monotonic as well.

Read logical time does not physically remove an entry. If `expiration_time <= read_logical_timestamp`, the entry is treated as nonexistent.

If the primary reads a record as deleted, a new primary will also read it as deleted after failover because the expiration point has passed and the clock offset is bounded.

Replica nodes cannot perform scheduled deletion independently. When no writes arrive, neither primary nor replica advances its write logical time, so garbage collection would stop. Submitting one empty log per minute advances the write logical time on both sides and triggers garbage collection. This avoids high-frequency logs while idle; the GC timing difference between primary and replica is then bounded by the empty-log interval. When no key is deleted, no deletion message needs to be sent to replicas.

> If primary and replica advance logical time outside Raft, for example with heartbeats, a replica that loses contact with the primary might delete an expired entry. The primary could then generate `SET NX` while the key is still live locally and send it after the replica has deleted the key. The two nodes could execute the command differently.

The scheme needs an assumption similar to Google's TrueTime API: the absolute clock offset between nodes must stay within an acceptable bound. Because cache-cat can tolerate several hundred milliseconds of error, NTP synchronization is normally sufficient.

## Algorithm Proof

### **Enumeration of All Read Scenarios**

> Ambiguity can arise during a primary-replica switch because the two local clocks cannot reach consensus. After the switch, however, the new primary's local clock must be greater than the old primary's clock before failure (leader-election time > clock offset). Time therefore keeps moving forward, and the externally visible read semantics never move backward.

A read on the primary has three possible outcomes:

1. No data is found and no value is returned. This is safe: the data either never existed or was deterministically deleted by time progression caused by an already committed write.
2. Data is found but has expired, so no value is returned.
3. Data is found and has not expired, so the value is returned.

Case 2 has two sub-cases:

- Both primary and replica have deleted the data. They are consistent.
- The primary has deleted the data but the replica has not. With bounded clock offset, this is harmless if no switch occurs. If a switch occurs, election time is greater than the clock difference, so the new primary also returns the data as expired.

Case 3 has two sub-cases:

- The replica expires slightly earlier than the primary. If the primary fails and the replica becomes primary, the read may find the data expired. This is acceptable because the old primary was already close to expiration, and a small expiration difference caused by clock offset is allowed.
- The replica expires slightly later than the primary. If the primary fails and the replica becomes primary, the new primary may still return the value. This is also safe because the value has not yet reached the replica's expiration point.

### Detailed Proof

The goal is to guarantee that once an operation determines that a key has expired, every later operation also treats that key as expired.

When a pair is set with expiration, it stores an absolute expiration point.

Operations that modify state-machine state are called write operations. Many writes are RMW operations and must read first. For example, Redis `APPEND` reads the current pair before modifying its value.

For a write, `expiration_time <= write_logical_timestamp` means that the value is expired. The write logical time is committed to the state machine through Raft and is generated with:

```text
max(write logical time, read logical time, physical time)
```

For a read, `expiration_time <= read_logical_timestamp` means that the value is expired; no extra consensus is started. The read logical time is kept in memory and uses the same maximum.

Write logical time is monotonic because every write change is committed through Raft and takes effect only after consensus.

Read logical time is monotonic on one machine. During a switch, cache-cat requires switch time to exceed the clock offset between all nodes, which can be achieved by configuring the Raft election timeout. The next primary's physical clock is therefore greater than the old primary's clock.

> The read and write logical clocks advance concurrently. While one thread generates a read clock, another may generate a larger write clock. The read may then see less data than it otherwise would have. This is acceptable because concurrent reads and writes do not require an order between those operations; either sequence number may be larger.
>
> A write clock is generated asynchronously before it is pushed into the queue, so a log with a larger clock can appear before a log with a smaller clock. This is allowed. Every clock is computed with `max(write logical time, read logical time, physical time)`, so it never decreases. If this situation occurs, all nodes make the same choice and leave the write clock unchanged. Unlike HLC, the write logical clock need not be unique; it only establishes consistent behavior.

We can therefore deduce:

- A write received after another write has a logical time greater than the earlier write, guaranteed by Raft state-machine iteration.
- A write received after a read returns has a logical time greater than that read.
- A read received after a write returns has a logical time greater than that write.
- A read received after another read returns has a logical time greater than the earlier read.

Therefore:

- After a write reports that data is expired, later writes also treat it as expired.
- After a read reports that data is expired, later writes also treat it as expired.
- After a write determines that data is expired, later reads also treat it as expired.
- After a read reports that data is expired, later reads also treat it as expired.

## Deterministic Iteration Algorithm

Logical clocks make individual write operations deterministic. Combining reads and writes in a Lua script can still make the final state diverge between primary and replica nodes. For example:

```lua
local field = redis.call("HRANDFIELD", KEYS[1])
if not field then
    return false
end

redis.call("HSET", KEYS[1], field, ARGV[1])
return {field, ARGV[1]}
```

A standalone read can use only the read logical clock; it cannot use the write logical clock. A Lua script is treated as a write command and therefore carries a write logical clock. While executing a Lua script, all read logical clocks are converted to the write logical clock so every node produces the same result.

Operations that are random use the read or write logical clock as the random seed. Hash iteration order is not stable, so unordered hash replies used inside Lua are sorted before they affect the script result. Ordinary reads do not need this heavier deterministic path.

The overall framework is:

```rust
pub(crate) fn execute_read_with_mode<C: ReadCommand, const DETERMINISTIC: bool>(
    &self,
    cmd: C,
    db_number: u16,
    read_clock: u64,
) -> Value {
    let cache = match self.databases.get(db_number as usize) {
        None => return Value::error("Key not found"),
        Some(v) => &v.mocha,
    };
    let key = cmd.key();
    let option = cache.get_with_read_clock(key, Some(read_clock));
    if DETERMINISTIC {
        cmd.execute_with_clock(option, read_clock)
    } else {
        cmd.execute(option)
    }
}
```
