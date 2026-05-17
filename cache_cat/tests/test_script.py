import redis

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)

# Lua 脚本
lua_script = """
local key = KEYS[1]
local value = ARGV[1]

redis.call('SET', key, value)

return redis.call('GET', key)
"""

# 1. 加载脚本，返回 SHA1
sha1 = r.script_load(lua_script)

print("script sha1:", sha1)

# 2. 使用 evalsha 执行
result = r.evalsha(
    sha1,
    1,          # KEYS 数量
    "test:key", # KEYS[1]
    "hello"     # ARGV[1]
)

print("result:", result)