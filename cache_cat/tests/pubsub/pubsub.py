import redis
import time

# 连接 Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 创建 pubsub
pubsub = r.pubsub()

# 订阅频道
pubsub.subscribe('news')
pubsub.subscribe('chat')

# 模式订阅
pubsub.psubscribe('user:*')

# 等一下，让订阅生效
time.sleep(1)

# -----------------------------
# PUBSUB CHANNELS
# 查看当前活跃频道
# -----------------------------
print("PUBSUB CHANNELS:")
print(r.pubsub_channels())

# -----------------------------
# PUBSUB NUMSUB
# 查看频道订阅数量
# -----------------------------
print("\nPUBSUB NUMSUB:")
print(r.pubsub_numsub('news', 'chat'))

# -----------------------------
# PUBSUB NUMPAT
# 查看模式订阅数量
# -----------------------------
print("\nPUBSUB NUMPAT:")
print(r.pubsub_numpat())

# 关闭
pubsub.close()