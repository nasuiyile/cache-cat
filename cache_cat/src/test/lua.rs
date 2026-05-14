use mlua::{Lua, Value, Error as LuaError, Variadic};
use moka::sync::Cache;
use std::sync::Arc;

pub mod lua {
    use super::*;

    #[test]
    fn test_redis_commands() {
        let lua_env = LuaEnv::new().unwrap();

        // 测试 SET
        let set_script = r#"
            redis.call('SET', 'mykey', 'hello world')
            return redis.call('GET', 'mykey')
        "#;
        let result = lua_env.exec_lua(set_script).unwrap();
        println!("SET/GET result: {:?}", result);

        // 测试 GET 不存在的 key
        let get_missing = r#"
            return redis.call('GET', 'nonexistent')
        "#;
        let result = lua_env.exec_lua(get_missing).unwrap();
        println!("GET missing: {:?}", result); // 应该是 nil

        // 测试多个参数
        let test_del = r#"
            redis.call('SET', 'key1', 'val1')
            redis.call('SET', 'key2', 'val2')
            local count = redis.call('DEL', 'key1', 'key2', 'key3')
            return count
        "#;
        let result = lua_env.exec_lua(test_del).unwrap();
        println!("DEL count: {:?}", result);
    }

    struct LuaEnv {
        lua: Lua,
        redis_handler: RedisHandler,
    }

    #[derive(Clone)]
    struct RedisHandler {
        cache: Arc<Cache<String, String>>,
    }

    impl RedisHandler {
        fn new() -> Self {
            RedisHandler {
                cache: Arc::new(
                    Cache::builder()
                        .max_capacity(10_000)
                        .build()
                ),
            }
        }

        fn call(&self, lua: &Lua, args: Variadic<String>) -> mlua::Result<Value> {
            if args.is_empty() {
                return Err(LuaError::external("redis.call requires at least one argument"));
            }

            let command = args[0].to_uppercase();

            match command.as_str() {
                "GET" => self.handle_get(lua, &args),
                "SET" => self.handle_set(lua, &args),
                "DEL" => self.handle_del(lua, &args),
                "EXISTS" => self.handle_exists(lua, &args),
                _ => Err(LuaError::external(format!(
                    "Unknown Redis command: {}", command
                ))),
            }
        }

        fn handle_get(&self, lua: &Lua, args: &[String]) -> mlua::Result<Value> {
            if args.len() != 2 {
                return Err(LuaError::external("ERR wrong number of arguments for 'GET' command"));
            }

            let key = &args[1];
            match self.cache.get(key) {
                Some(value) => Ok(Value::String(lua.create_string(&value)?)),
                None => Ok(Value::Nil),
            }
        }

        fn handle_set(&self, lua: &Lua, args: &[String]) -> mlua::Result<Value> {
            if args.len() < 3 {
                return Err(LuaError::external(
                    "ERR wrong number of arguments for 'SET' command"
                ));
            }

            let key = &args[1];
            let value = &args[2];

            self.cache.insert(key.clone(), value.clone());
            Ok(Value::String(lua.create_string("OK")?))
        }

        fn handle_del(&self, _lua: &Lua, args: &[String]) -> mlua::Result<Value> {
            let mut count = 0i64;
            for key in &args[1..] {
                self.cache.invalidate(key);
                count += 1;
            }
            Ok(Value::Integer(count))
        }

        fn handle_exists(&self, _lua: &Lua, args: &[String]) -> mlua::Result<Value> {
            let mut count = 0i64;
            for key in &args[1..] {
                if self.cache.contains_key(key) {
                    count += 1;
                }
            }
            Ok(Value::Integer(count))
        }
    }

    impl LuaEnv {
        fn new() -> mlua::Result<LuaEnv> {
            let lua = Lua::new();

            // 沙箱设置
            let globals = lua.globals();
            globals.set("os", Value::Nil)?;
            globals.set("io", Value::Nil)?;
            globals.set("package", Value::Nil)?;
            globals.set("require", Value::Nil)?;
            globals.set("dofile", Value::Nil)?;
            globals.set("loadfile", Value::Nil)?;

            let handler = RedisHandler::new();
            let redis_api = lua.create_table()?;

            let handler_clone = handler.clone();
            redis_api.set(
                "call",
                lua.create_function(move |lua_ctx, args: Variadic<String>| {
                    handler_clone.call(lua_ctx, args)
                })?,
            )?;

            globals.set("redis", redis_api)?;

            Ok(LuaEnv {
                lua,
                redis_handler: handler,
            })
        }

        fn exec_lua(&self, cmd: &str) -> mlua::Result<Value> {
            let result: Value = self.lua.load(cmd).eval()?;
            Ok(result)
        }
    }
}