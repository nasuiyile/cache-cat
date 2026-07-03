use crate::error::ProtocolError;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::mocha::request_handler::do_request;
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;
use lru::LruCache;
use mlua::prelude::LuaError;
use mlua::{HookTriggers, Lua, Value as LuaValue, Variadic, VmState};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// LRU cache capacity
const SCRIPT_CACHE_CAPACITY: usize = 500;

const MAX_MEM: usize = 10 * 1024 * 1024;
#[derive(Debug)]
pub struct LuaEnv {
    lua: Lua,
    raft_command: RaftCommandFactory,
    // Script content → Cache of compiled functions
    script_cache: Mutex<LruCache<String, mlua::Function>>,
    pub script_map: Mutex<HashMap<String, String>>,
    // Set to false at runtime, set to true at end
    interrupt_flag: Arc<AtomicBool>,
}

impl LuaEnv {
    // Return whether the current execution is in progress
    pub fn interrupt(&self) -> bool {
        self.interrupt_flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
    pub fn new() -> Result<LuaEnv, ProtocolError> {
        let lua = Lua::new();
        lua.set_memory_limit(MAX_MEM)?;
        // Sandbox setup (as above)
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

        // Initialize LRU cache with a capacity of 500
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

    /// Execute Lua script, similar to Redis EVAL
    ///
    /// * `cache`   - Current Mocha cache reference
    /// * `script`  - Lua script content
    /// * `keys`    - KEYS table passed to script (subscripts starting from 1)
    /// * `args`    - ARGV table passed to script (subscript starting from 1)
    /// * `update`  - Update object used to record modifications
    ///
    /// The Lua virtual machine of mlua only runs on one thread,
    /// and the redis.call and redis.call Lua functions are not called simultaneously.
    ///
    /// Every time the script is executed, it is either in call or pcall,
    /// and there is no possibility of concurrent access to update.
    pub fn exec_lua(
        &self,
        cache: &MyCache,
        script: &str,
        keys: &[Bytes],
        args: &[Bytes],
        update: &mut Update,
    ) -> Result<Value, ProtocolError> {
        self.interrupt_flag.store(false, Ordering::SeqCst);

        // 2. Set hooks
        // HookTriggers::default().every_count(1000) Triggered every 1000 instructions executed
        let flag = self.interrupt_flag.clone();
        // Note: According to the function signature you provided,
        // the callback function accepts two parameters (lua, debug)
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
        // Convert &mut Update to a bare pointer,
        // allowing multiple closures to capture the same object
        let update_ptr: *mut Update = update;
        let res = self.lua.scope(|scope| -> mlua::Result<LuaValue> {
            // ---- redis.call ----
            // Errors will be thrown directly upwards, interrupting script execution
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
                    // - The Lua virtual machine is single threaded, and redis.call
                    //   and redis.call will never be called concurrently.
                    // - The mutable reference obtained here only exists during
                    //   the current closure call and will not escape to the outside.
                    let update = unsafe { &mut *update_ptr };

                    let operation = self
                        .raft_command
                        .parse_request(&vec)
                        .map_err(LuaError::external)?;
                    let value = do_request(cache, operation, update, false);
                    if let Value::Error(e) = value {
                        return Err(LuaError::external(e));
                    }
                    value.into_lua_value(&self.lua)
                })?;

            // ---- redis.pcall ----
            // Errors will be packaged as {err="..."} and returned to Lua,
            // and the script can continue to execute
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
                    let update = unsafe { &mut *update_ptr };

                    match self.raft_command.parse_request(&vec) {
                        Ok(operation) => {
                            do_request(cache, operation, update, false).into_lua_value(&self.lua)
                        }
                        Err(e) => {
                            let err_table = self.lua.create_table()?;
                            err_table.set("err", e.to_string())?;
                            Ok(LuaValue::Table(err_table))
                        }
                    }
                })?;

            // ---- Inject global Redis table ----
            let redis_table = self.lua.create_table()?;
            redis_table.set("call", redis_call)?;
            redis_table.set("pcall", redis_pcall)?;
            self.lua.globals().set("redis", redis_table)?;

            // ---- KEYS ----
            let keys_table = self.lua.create_table()?;
            for (i, key) in keys.iter().enumerate() {
                let lua_key = self.lua.create_string(key.as_ref())?;
                keys_table.set(i + 1, lua_key)?;
            }
            self.lua.globals().set("KEYS", keys_table)?;

            // ---- ARGV ----
            let argv_table = self.lua.create_table()?;
            for (i, arg) in args.iter().enumerate() {
                let lua_arg = self.lua.create_string(arg.as_ref())?;
                argv_table.set(i + 1, lua_arg)?;
            }
            self.lua.globals().set("ARGV", argv_table)?;

            // Execute pre compiled scripts
            func.call::<LuaValue>(())
        });
        self.interrupt_flag.store(true, Ordering::SeqCst);

        // Map Lua return values back to the internal Value type
        Value::from_lua(res?, &self.lua)
    }

    /// Retrieve compiled functions from cache,
    /// if not available, compile and store them in cache (LRU elimination)
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
