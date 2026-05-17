from time import sleep
import redis

r = redis.Redis(
    db=0,
    host='localhost',
    port=6379,
    decode_responses=True
)
print()

r.set('key_lua', '0')

# Lua 脚本：获取比当前值大的下一个质数并设置回去
lua_script = """
local current = tonumber(redis.call('GET', KEYS[1]))
if current == nil then
    return nil
end

local function is_prime(n)
    if n <= 1 then
        return false
    end
    if n <= 3 then
        return true
    end
    if n % 2 == 0 or n % 3 == 0 then
        return false
    end
    local i = 5
    while i * i <= n do
        if n % i == 0 or n % (i + 2) == 0 then
            return false
        end
        i = i + 6
    end
    return true
end

local next_num = current + 1
while not is_prime(next_num) do
    next_num = next_num + 1
end

redis.call('SET', KEYS[1], next_num)
return next_num
"""

# 使用 eval 执行 Lua 脚本
result = r.eval(lua_script, 1, 'key_lua')
print(f"下一个质数是: {result}")

# 验证结果
print(f"key_lua 的值现在是: {r.get('key_lua')}")

r.set('key_testtest', '0')
lua_script = """
-- 尝试在 Lua 脚本中调用 EVAL（原生redis会报错）
local result = redis.call('EVAL', "return redis.call('GET','key_testtest')", 0)
return result
"""
try:
    result = r.eval(lua_script, 0)
    print(f"结果: {result}")
except redis.exceptions.ResponseError as e:
    print(f"预期错误: {e}")

r.lpush('list_lua', '1', '2', '3')

# 测试pcall
lua_script = """
redis.pcall('GET', 'list_lua')
local result = redis.pcall('LRANGE', 'list_lua', 0, -1)
return result
 """
result = r.eval(lua_script, 0)
print(f"结果: {result}")
