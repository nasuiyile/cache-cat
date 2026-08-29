from time import sleep

import redis

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)
r.flushdb()
print()

r.set('test0', 'test0')

r.lpush('test1', 'test')
print(r.lrange('test1', 0, -1))

r.hset('test2', 'test', 'test')
print(r.hget('test2', 'test'))

print(r.zadd("my_zset", {"a": 1, "b": 2, "c": 3}))

print(r.hincrby("test5", "test", 1))
print(r.hget("test5", "test"))
print(r.exists("test5"))

r.set('test6', 'test')
print(r.rename('test6', 'test7'))
print(r.get('test7'))
# 1秒过期
r.set('test10', 'test---------')
print(r.expire('test10', 1))
print(r.get('test10'))
r.persist('test10')
print(r.get('test10'))

r.set('test11', 'test')
r = redis.Redis(
    db=15,
    host='localhost',
    port=6379,
    decode_responses=True
)
r.set('test12', 'test')
print(r.get('test12'))
print(r.echo('test13'))

r.sadd('test14', 'test')
print(r.smembers('test14'))
print("----")
print(r.sismember('test14', 'test'))

r.hset('test15', 'test', 'test')
r.hmget('test15', ['test'])

r.hset('test16', 'test', 'test')
r.hdel('test16', 'test')
print(r.hget('test16', 'test'))

print(r.srem('test14', 'test'))
print(r.smembers('test20'))

key = "bitmap_test"
r.setbit(key, 0, 1)
# 设置第 7 位为 1
r.setbit(key, 7, 1)
# 设置第 10 位为 1
r.setbit(key, 10, 1)

print(r.getbit(key, 0))
print(r.getbit(key, 7))
print(r.getbit(key, 8))
print(r.getbit(key, 10))

print(r.time())

r.delete("my_zset")
r.zadd("my_zset", {"a": 1, "b": 2, "c": 3})
print(r.zrange("my_zset", 0, -1))
print(r.zrangebyscore("my_zset", 1, 2))

r.psetex(
    name="user:1",
    time_ms=50,
    value="Bella"
)

r.set("my_key", "1111")
# SETNX: 只有在key不存在时才设置
result = r.setnx("my_key", 'my_value')

print(r.get("my_key"))

r.renamenx("my_key", "my_key2")
print(r.get("my_key2"))
r.set("my_key2", "测试test")

print(r.strlen("my_key2"))

r.hset('test2', 'test', 'test1')
r.hset('test2', 'test2', 'test2')
print(r.hgetall('test3'))

print(r.hkeys('test3'))
print(r.hvals('test2'))

print(r.mget(["test12", "test12"]))

r.lpush("list test1", "test")
print(r.llen("list test1"))

#
r.set("test20", "test20")
r.expire("test20", 1)
sleep(0.5)
print(r.get("test20"))

r.rpush("list test2", "test")
print(r.rpush("list test2", "test2"))
print(r.lrange("list test2", 0, -1))

print(r.type("test20"))
r.lrem("list test2", 1, "test")
print(r.lrange("list test2", 0, -1))
r.lset("list test2", 0, "test1")
print(r.lrange("list test2", 0, -1))
r.hset("test2", "test", "test1")
print(r.hexists("test2", "test100"))
r.set("test21", "3")
r.decrby("test21", 1)
print(r.get("test21"))
r.set("test22", "3", ex=10)
print(r.pttl("test22"))
print(r.ttl("test22"))

r.hset("test23", "test34", "test1")
print(r.setnx("test23", "test2"))
print(r.hlen("test23"))

r.set("test24", "0")
r.decr("test24")
print(r.get("test24"))
print(r.getset("test24", "1"))
print(r.get("test24"))

r.zadd("my_zset", {"a": 1, "b": 2, "c": 3})
r.zrem("my_zset", "a")
print(r.zrange("my_zset", 0, -1))

r.lpush("list test3", "test")
r.lpush("list test3", "test2")
r.lpush("list test3", "test3")
r.ltrim("list test3", 0, 1)
print(r.lrange("list test3", 0, -1))

print(r.get("test24"))
r.set("test24", "test24")
print(r.get("test24"))
r.setbit("test25", 0, 1)
print(r.bitcount("test25", 0, -1))

r.setbit("test26", 0, 0)  # 第0位设为0（默认就是0，这行可省略）
r.setbit("test26", 1, 1)  # 第1位设为1
print(r.bitpos("test26", 1))  # 返回 1

r.sadd("test27", "test")
print(r.scard("test27"))

result = (r.bitfield('player:1002:stats')
          .set('u32', 0, 100).incrby('u16', 32, 50)
          .get('u32', 0).get('u16', 32)
          .incrby('u16', 48, 1).get('u16', 48)).execute()

print(f"SET结果: {result[0]}")  # 0
print(f"金币自增结果: {result[1]}")  # 50
print(f"等级读取: {result[2]}")  # 100
print(f"金币读取: {result[3]}")  # 50
print(f"经验自增结果: {result[4]}")  # 1ZCARD
print(f"经验读取: {result[5]}")  # 1

r.sadd("test27", "test")
r.spop("test27")
print(r.smembers("test27"))
r.sadd("test27", "test")
print(r.srandmember("test27"))

print(r.keys('test*'))

r.zadd("my_zset", {"a": 1, "b": 2, "c": 3})
print(r.zscore("my_zset", "b"))
print(r.zcard("my_zset"))

r.set("test28", "test")
print(r.unlink("test28"))
print(r.get("test28"))

r.zadd("my_zset", {"a": 1, "b": 2, "c": 3})
print(r.zrevrank("my_zset", "a"))
print(r.dbsize())

r.set("test30", "test")
# print(r.memory_usage("test30"))
# print(r.memory_stats())
#
# print(r.memory_purge())
#
# print(r.memory_malloc_stats())


r.pfadd("uv:page1", "user1", "user2", "user3")
r.pfadd("uv:page1", "user2", "user4")

# 2. PFCOUNT：估算去重后的数量
print(r.pfcount("uv:page1"))
# 大约是 4

# 再创建一个 HyperLogLog
r.pfadd("uv:page2", "user3", "user4", "user5")

print(r.pfcount("uv:page2"))
# 大约是 3

# 3. PFMERGE：合并多个 HyperLogLog
r.pfmerge("uv:all", "uv:page1", "uv:page2")

print(r.pfcount("uv:all"))
# 大约是 5

r = redis.Redis(host='localhost', port=6379, decode_responses=False)
# 设置字节值
r.set("a", bytes([0b00001111]))  # 0x0F = 15
r.set("b", bytes([0b00110011]))  # 0x33 = 51

# 执行位运算
r.bitop("AND", "result", "a", "b")

# 获取结果
value = r.get("result")
print(value)  # b'\x03'
print(bin(value[0]))  # 0b11

# 位运算解释：
# a = 00001111
# b = 00110011
# AND 结果 = 00000011 = 3

print(r.bf().add("bf1", "user1"))
print(r.bf().exists("bf1", "user1"))
