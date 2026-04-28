import redis
import time

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

# 设置 key，1 秒后过期
# r.set('name', 1, ex=10)
# r.ping()
# 等 5 秒
# time.sleep(5)
# 再获取
# r.delete('name')
# r.incr('name')
r.mset({'test3': '1', 'test2': '2'})
# print(r.get('123123'))
print(r.mget('test3', 'test111111'))
# print(r.get('name'))
