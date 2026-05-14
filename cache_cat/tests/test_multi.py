from time import sleep

import redis

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)
r.set('name', 'Alice')

pipe = r.pipeline()
pipe.multi()  # 等同于执行 MULTI 命令

# 在事务中添加多个命令
pipe.set('name', 'Alice')
pipe.set('age', 25)
pipe.incr('age')  # age 变为 26
pipe.get('name')
pipe.get('age')

# 执行事务
result = pipe.execute()

# 输出返回的结果
print(result)
# 或者逐行输出
for i, res in enumerate(result):
    print(f"命令 {i+1} 的结果: {res}")