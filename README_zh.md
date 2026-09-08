# cache-cat

<div align="center">
<a href="https://github.com/nasuiyile/cache-cat/blob/master/README.md">English</a> ｜
<a href="https://github.com/nasuiyile/cache-cat/blob/master/README_zh.md">简体中文</a> |
<a href="https://nasuiyile.github.io/cache-cat-website">官网</a> 
</div>


## 介绍

cache-cat是一个高性能的键值对缓存库，同时使用Raft协议确保高可用的同时确保数据的强一致性。

cache-cat想实现一个绝对高性能的基于raft协议容灾的缓存框架。相比于Redis，Memcache等缓存，Cache-cat能够保证不会丢失数据。在这方面定位最类似的是

[RedisLabs/redisraft: A Redis Module that make it possible to create a consistent Raft cluster from multiple Redis instances.](https://github.com/RedisLabs/redisraft)

但是该项目只是一个lab项目。

> 即便使用了Redis的各种集群策略。Redis仍然有可能丢数据，Redis的集群解决的是可用性问题，而不解决数据一致性问题。

相比于ETCD，Zookeeper，Consul这类更为人熟知的注册中心，他们同样使用共识算法。
并且能够保证数据的可靠性。然而，在相同环境和默认配置下，这类系统的性能和延迟仍无法与 Cache-Cat 相比。例如，Cache-Cat 的写入吞吐量可达到约 500k/s，而 TiKV 约为 200k/s。此外，这些中间件在设计之初并非面向缓存场景，因此缺乏许多缓存系统所需的特性，例如 LRU、LFU 等数据淘汰策略，以及最大内存占用限制等功能。

## 基准测试

YCSB 吞吐量基准测试结果：

![YCSB 吞吐量基准测试](benchmark/res/ycsb_throughput.png)

## 功能

对于许多小公司来说，你可能只想构建一个高可用的应用，而不想引入成吨的中间件服务。

理论上你可以使用cache-cat来做

- 键值对数据库：类似TIKV，用来存储一些关键不能丢失的数据。
- 服务发现（注册中心），配置中心：类似consul和Zookeeper的定位，作为注册和配置中心来进行实现。需客户端适配。
- 缓存：类似Redis，dragonfly，valkey的定位：缓存绝大部分是读取操作，在读取层面，cache-cat的性能不会弱于这些缓存。而写入操作（根据一份调研，大部分系统的读写比为95：5，并且缓存本身无法加速写操作）cache-cat仍然提供了500k+的每秒处理速度，对于绝大部分场景足够使用。
- 分布式锁：类似Zookeeper和ETCD。但不同于Redis的分布式锁，Redis的分布式锁相对来说有更多问题（即便你使用的是Redlock：[Is Redlock safe? - ](https://antirez.com/news/101)）。

**使用场景**:当你需要数据不会产生丢失的场景时，你应该选择Cache-cat。例如，你想将所有的用户长期保存在内存中，方便读取。你需要一把分布式锁，并且不想这把锁因为节点故障导致锁失效。你想要存放一些关键性的配置，并且不希望这个配置因为节点故障被删除。

对于很大一部分场景，你仍然可以选择Redis等传统缓存系统。比如你想将总是需要被访问的配置文件等原本存放在数据库中的数据放到缓存预热。

性能基准测试结果见上方[基准测试](#基准测试)部分，更完整的benchmark会在cache-cat功能完善后补全。

## 一致性模型

> 一个常被人提起的一致性问题是关于缓存与数据库的双写一致性。
>
> [干货 | 携程最终一致和强一致性缓存实践](https://mp.weixin.qq.com/s/E-chAZyHtaZOdA19mW59-Q)
>
> 这与数据库本身的一致性存在区别，而下文提到的一致性为数据库本身的一致性模型，而不是数据库与缓存的双写一致性模型。
>
> 简而言之，如果一次写入操作能被立即读取到最新写入的数据，则可以被看做强一致。（对外暴露的语义一定是单个状态机）
>
> 如果需要一段时间后才能读取到最新的数据，则可以被看作为最终一致性。

## QA

您可能会好奇cahce-cat与以下框架的区别和关系，下文会一一解释。

问题：和Tikv有什么区别：

答：Tikv是一个使用raft-engine作为日志复制，RocketDB作为存储层的数据库实现 。cache-cat为了性能，状态机数据不会被保存在磁盘中。

***

问题：Redis真的会丢数据吗，使用集群策略呢？

无论是Cluster还是Sentinel模式Redis的集群策略并没有采用共识算法。处理顺序为先回复请求再同步给从节点。如果在回复请求后没来得及发给从节点就宕机，Redis选出新的主之后这条数据就会永远的丢失。此外Redis集群还有脑裂问题。

***

问题：Raft协议强制将数据写入磁盘并且通知从节点之后才能将数据返回。这是否与缓存的高效设计背道而驰？

答：准确的说Raft是将操作日志写入磁盘，Raft并不限定状态机的数据是存放在哪里的，在cacahe-cat中这部分数据是完全存放在内存中的（hashmap和其他数据结构）。如果要进行类比的化，可以将需要持久化的数据比作Redis的AOF日志。作为参考，Zookeeper的整个Znode树以及consul的键值对都是存储在内存中的。ETCD则是会将数据持久化到磁盘。
对于缓存而言，同步和刷盘操作会带来写操作的延迟提升，相比于相同实现的纯缓存库这是必然的。但对于读取操作无需进行任何额外的磁盘操作。我们认为缓存更关注的是读取延迟而不是写入延迟，并且我们认为这样做是值得的：用略高的写入延迟带来的是完全不会丢弃数据。

This project includes code derived from https://github.com/lichuang/coredb ,  https://github.com/lichuang/rockraft .

licensed under the Apache License 2.0.

























