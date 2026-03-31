import redis

r = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True  # 自动把 bytes 转成 str
)
r.set('name', 'hello')
print(r.get('name'))
