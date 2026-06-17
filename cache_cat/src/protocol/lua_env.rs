use crate::error::ProtocolError;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::mocha::request_handler::do_request;
use crate::raft::types::core::response_value::Value;
use lru::LruCache;
use mlua::prelude::LuaError;
use mlua::{HookTriggers, Lua, Value as LuaValue, Variadic, VmState};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// LRU 缓存容量
const SCRIPT_CACHE_CAPACITY: usize = 500;

const MAX_MEM: usize = 10 * 1024 * 1024;
#[derive(Debug)]
pub struct LuaEnv {
    lua: Lua,
    raft_command: RaftCommandFactory,
    // 脚本内容 → 已编译函数的缓存
    script_cache: Mutex<LruCache<String, mlua::Function>>,
    pub script_map: Mutex<HashMap<String, String>>,
    // 运行时设置为false，结束了设置为true
    interrupt_flag: Arc<AtomicBool>,
}

impl LuaEnv {
    //返回当前是否在执行
    pub fn interrupt(&self) -> bool {
        self.interrupt_flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
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
            script_map: Mutex::new(HashMap::new()),
            interrupt_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    /// 执行 Lua 脚本，类似 Redis EVAL
    ///
    /// * `cache`   - 当前 Mocha cache 的引用
    /// * `script`  - Lua 脚本内容
    /// * `keys`    - 传递给脚本的 KEYS 表（下标从 1 开始）
    /// * `args`    - 传递给脚本的 ARGV 表（下标从 1 开始）
    /// * `update`  - 用于记录修改的 Update 对象
    /// mlua 的 Lua 虚拟机只在一个线程上运行，redis.call 和 redis.pcall 这两个 Lua 函数不会被同时调用。
    /// 每次脚本执行时，要么在 call 里，要么在 pcall 里，不存在并发访问 update 的可能。
    pub fn exec_lua(
        &self,
        cache: &MyCache,
        script: &str,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        update: &mut Update,
    ) -> Result<Value, ProtocolError> {
        self.interrupt_flag.store(false, Ordering::SeqCst);

        // 2. 设置钩子
        // HookTriggers::default().every_count(1000) 表示每执行 1000 条指令触发一次
        let flag = self.interrupt_flag.clone();
        // 注意：根据你提供的函数签名，回调函数接受 (lua, debug) 两个参数
        self.lua.set_hook(
            HookTriggers::default().every_nth_instruction(1000),
            move |_lua, _debug| {
                if flag.load(Ordering::Relaxed) {
                    return Err(LuaError::external("script interrupted by host"));
                }
                Ok(VmState::Continue)
            },
        )?;

        let func = self.get_or_compile_script(script)?;
        // 将 &mut Update 转换成裸指针，允许多个闭包捕获同一对象
        let update_ptr: *mut Update = update;
        let res = self.lua.scope(|scope| -> mlua::Result<LuaValue> {
            // ---- redis.call ----
            // 错误会直接向上抛出，中断脚本执行
            let redis_call =
                scope.create_function_mut(move |_lua_ctx, args: Variadic<String>| {
                    if args.is_empty() {
                        return Err(LuaError::external(
                            "redis.call requires at least one argument",
                        ));
                    }
                    let mut vec = Vec::new();
                    for param in args {
                        vec.push(Value::SimpleString(param));
                    }

                    // SAFETY:
                    // - Lua 虚拟机是单线程的，redis.call 和 redis.pcall 绝不会并发调用。
                    // - 此处获取的可变引用只在当前闭包调用期间存活，不会逃逸到外部。
                    #[allow(unsafe_code)]
                    let update = unsafe { &mut *update_ptr };

                    let operation = self
                        .raft_command
                        .parse_request(&vec)
                        .map_err(|e| LuaError::external(e))?;
                    let value = do_request(cache, operation, update, false);
                    if let Value::Error(e) = value {
                        return Err(LuaError::external(e));
                    }
                    value.into_lua_value(&self.lua)
                })?;

            // ---- redis.pcall ----
            // 错误会被包装成 {err = "..."} 返回给 Lua，脚本可以继续执行
            let redis_pcall =
                scope.create_function_mut(move |_lua_ctx, args: Variadic<String>| {
                    if args.is_empty() {
                        return Err(LuaError::external(
                            "redis.pcall requires at least one argument",
                        ));
                    }
                    let mut vec = Vec::new();
                    for param in args {
                        vec.push(Value::SimpleString(param));
                    }
                    #[allow(unsafe_code)]
                    let update = unsafe { &mut *update_ptr };
                    let result = match self.raft_command.parse_request(&vec) {
                        Ok(operation) => {
                            do_request(cache, operation, update, false).into_lua_value(&self.lua)
                        }
                        Err(e) => {
                            let err_table = self.lua.create_table()?;
                            err_table.set("err", e.to_string())?;
                            Ok(LuaValue::Table(err_table))
                        }
                    };
                    result
                })?;

            // ---- 注入全局 redis 表 ----
            let redis_table = self.lua.create_table()?;
            redis_table.set("call", redis_call)?;
            redis_table.set("pcall", redis_pcall)?;
            self.lua.globals().set("redis", redis_table)?;

            // ---- KEYS ----
            let keys_table = self.lua.create_table()?;
            for (i, key) in keys.iter().enumerate() {
                let lua_key = self.lua.create_string(key.as_slice())?;
                keys_table.set(i + 1, lua_key)?;
            }
            self.lua.globals().set("KEYS", keys_table)?;

            // ---- ARGV ----
            let argv_table = self.lua.create_table()?;
            for (i, arg) in args.iter().enumerate() {
                let lua_arg = self.lua.create_string(arg.as_slice())?;
                argv_table.set(i + 1, lua_arg)?;
            }
            self.lua.globals().set("ARGV", argv_table)?;

            // 执行预先编译好的脚本
            func.call::<LuaValue>(())
        });
        self.interrupt_flag.store(true, Ordering::SeqCst);

        // 将 Lua 返回值映射回内部 Value 类型
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
