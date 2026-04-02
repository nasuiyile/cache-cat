import redis
import time

r = redis.Redis(
    # db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

# 设置 key，1 秒后过期
r.set('name', 'hello', ex=1)
r.ping()
# 等 5 秒
# time.sleep(5)
# 再获取
r.delete('name')

print(r.get('name'))
