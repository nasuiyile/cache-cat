# 集群访问策略

## 客户端连接策略

> 下文的客户端指的都是需要连接cache-cat的客户端。该文档只是作为构想，尚未完全实现。

理论上存在多种通过外部方案结合实现高可用的方案。
如通过proxy来代理转发进行主从切换。无状态的proxy可以主动和集群内的全部节点通信来确认是否存活。
或者通过Kubernetes，虽然k8s本身不知道谁是primary谁是replica但可以通过operator方案来实现：https://github.com/dragonflydb/dragonfly-operator。
但是以上方案对于cache-cat来说均过于复杂。cache-cat选择直接兼容Redis的哨兵命令。直接将集群内的所有节点配置为哨兵节点。在cache-cat中主节点和从节点都可以响应哨兵相关的基础命令。