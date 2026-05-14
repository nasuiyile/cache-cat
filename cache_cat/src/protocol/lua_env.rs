use crate::error::ProtocolError;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::raft::types::core::moka::moka::{MyCache, Update};
use crate::raft::types::core::moka::request_handler::do_request;
use crate::raft::types::core::response_value::Value;
use mlua::prelude::LuaError;
use mlua::{Lua, Value as LuaValue, Variadic};

use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
// 使用 lru crate

/// LRU 缓存容量
const SCRIPT_CACHE_CAPACITY: usize = 500;

const MAX_MEM: usize = 10 * 1024 * 1024;
#[derive(Debug)]
pub struct LuaEnv {
    lua: Lua,
    raft_command: RaftCommandFactory,
    // 脚本内容 → 已编译函数的缓存
    script_cache: Mutex<LruCache<String, mlua::Function>>,
}

impl LuaEnv {
    pub fn new() -> Result<LuaEnv, ProtocolError> {
        let lua = Lua::new();
        lua.set_memory_limit(MAX_MEM)?;
        // 沙箱设置（同上）
        let globals = lua.globals();
        globals.set("os", LuaValue::Nil)?;
        globals.set("io", LuaValue::Nil)?;
        globals.set("package", LuaValue::Nil)?;
        globals.set("require", LuaValue::Nil)?;
        globals.set("dofile", LuaValue::Nil)?;
        globals.set("loadfile", LuaValue::Nil)?;
        globals.set("debug", LuaValue::Nil)?;
        globals.set("coroutine", LuaValue::Nil)?;
        globals.set("load", LuaValue::Nil)?;

        // 初始化 LRU 缓存，容量 500
        let cache =
            LruCache::new(NonZeroUsize::new(SCRIPT_CACHE_CAPACITY).expect("capacity must be > 0"));

        Ok(LuaEnv {
            lua,
            raft_command: RaftCommandFactory::init_lua(),
            script_cache: Mutex::new(cache),
        })
    }

    /// 执行 Lua 脚本，类似 Redis EVAL
    ///
    /// * `cache`   - 当前 Moka cache 的引用
    /// * `script`  - Lua 脚本内容
    /// * `keys`    - 传递给脚本的 KEYS 表（下标从 1 开始）
    /// * `args`    - 传递给脚本的 ARGV 表（下标从 1 开始）
    /// * `update`  - 用于记录修改的 Update 对象
    pub fn exec_lua(
        &self,
        cache: &MyCache,
        script: &str,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        update: &mut Update,
    ) -> Result<Value, ProtocolError> {
        // 获取或插入已编译的函数（缓存命中则直接使用）
        let func = self.get_or_compile_script(script)?;

        let res = self.lua.scope(|scope| -> mlua::Result<LuaValue> {
            // 1. 创建临时 redis.call 闭包（每次执行时捕获 cache、update）
            let redis_call = scope.create_function_mut(|_lua_ctx, args: Variadic<String>| {
                if args.is_empty() {
                    return Err(LuaError::external(
                        "redis.call requires at least one argument",
                    ));
                }
                let mut vec = Vec::new();
                for param in args {
                    vec.push(Value::SimpleString(param));
                }

                let operation = self
                    .raft_command
                    .parse_request(&vec)
                    .map_err(|e| LuaError::external(e))?;
                let value = do_request(cache, operation, update);
                value.into_lua_value(&self.lua)
            })?;

            // 2. 注入 redis 全局表
            let redis_table = self.lua.create_table()?;
            redis_table.set("call", redis_call)?;
            self.lua.globals().set("redis", redis_table)?;

            let keys_table = self.lua.create_table()?;
            for (i, key) in keys.iter().enumerate() {
                let lua_key = self.lua.create_string(key.as_slice())?;
                keys_table.set(i + 1, lua_key)?;
            }
            self.lua.globals().set("KEYS", keys_table)?;
            let argv_table = self.lua.create_table()?;
            for (i, arg) in args.iter().enumerate() {
                let lua_arg = self.lua.create_string(arg.as_slice())?;
                argv_table.set(i + 1, lua_arg)?;
            }
            self.lua.globals().set("ARGV", argv_table)?;

            // 5. 调用缓存的函数（无参数）
            func.call::<LuaValue>(())
        });

        // 将 Lua 返回值转换成内部 Value
        Value::from_lua(res?, &self.lua)
    }

    /// 从缓存获取已编译函数，若没有则编译并存入缓存（LRU 淘汰）
    fn get_or_compile_script(&self, script: &str) -> Result<mlua::Function, ProtocolError> {
        if let Some(func) = self.script_cache.lock().get(script) {
            return Ok(func.clone());
        }

        let func = self
            .lua
            .load(script)
            .into_function()
            .map_err(|e| ProtocolError::ScriptCompileError(e.to_string()))?;

        self.script_cache
            .lock()
            .put(script.to_owned(), func.clone());

        Ok(func)
    }
}
