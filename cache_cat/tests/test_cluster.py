from time import sleep

import redis

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

print()
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

r.hset('test15', 'test', 'test')
r.hmget('test15', ['test'])

r.hset('test16', 'test', 'test')
r.hdel('test16', 'test')
print(r.hget('test16', 'test'))

print(r.srem('test14', 'test'))
print(r.smembers('test20'))