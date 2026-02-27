use bytes::Bytes;
use dashmap::DashMap;
use mlua::prelude::*;
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::sync::Arc;
use tracing;

use solikv_core::CommandResponse;

use crate::CommandEngine;

/// Max Lua VMs cached per thread.
const LUA_POOL_MAX: usize = 8;

// ---------------------------------------------------------------------------
// Script cache
// ---------------------------------------------------------------------------

pub struct ScriptCache {
    scripts: DashMap<String, String>,
}

impl Default for ScriptCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: DashMap::new(),
        }
    }

    /// Cache a script and return its SHA1 hex digest.
    pub fn load(&self, source: &str) -> String {
        let sha = sha1_hex(source);
        self.scripts.insert(sha.clone(), source.to_owned());
        sha
    }

    /// Retrieve a cached script by SHA1.
    pub fn get(&self, sha: &str) -> Option<String> {
        self.scripts.get(sha).map(|r| r.value().clone())
    }

    /// Check existence of one or more SHA1 digests. Returns vec of booleans.
    pub fn exists(&self, shas: &[&str]) -> Vec<bool> {
        shas.iter().map(|s| self.scripts.contains_key(*s)).collect()
    }

    /// Clear the entire cache.
    pub fn flush(&self) {
        self.scripts.clear();
    }
}

// ---------------------------------------------------------------------------
// SHA1 helper
// ---------------------------------------------------------------------------

pub fn sha1_hex(input: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

// ---------------------------------------------------------------------------
// Blocked commands inside scripts
// ---------------------------------------------------------------------------

const BLOCKED_COMMANDS: &[&str] = &[
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "EVAL",
    "EVALSHA",
    "SCRIPT",
];

fn is_blocked_in_script(cmd: &str) -> bool {
    BLOCKED_COMMANDS.iter().any(|b| b.eq_ignore_ascii_case(cmd))
}

// ---------------------------------------------------------------------------
// Thread-local Lua VM pool
// ---------------------------------------------------------------------------

/// A pooled Lua VM tagged with its owning engine's identity.
struct PooledLua {
    lua: Lua,
    engine_id: usize,
}

thread_local! {
    static LUA_POOL: RefCell<Vec<PooledLua>> = const { RefCell::new(Vec::new()) };
}

/// Take a pre-sandboxed Lua VM from the thread-local pool, or create a new one.
fn take_lua(engine: &Arc<CommandEngine>) -> Lua {
    let engine_id = Arc::as_ptr(engine) as usize;
    let cached = LUA_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        pool.iter()
            .rposition(|p| p.engine_id == engine_id)
            .map(|idx| pool.swap_remove(idx).lua)
    });

    if let Some(lua) = cached {
        return lua;
    }

    // Cold path: create, sandbox, and register redis.* once
    let lua = Lua::new();
    sandbox_lua(&lua);
    setup_redis_module(&lua, engine);
    lua
}

/// Return a Lua VM to the thread-local pool for reuse.
fn return_lua(lua: Lua, engine: &Arc<CommandEngine>) {
    let engine_id = Arc::as_ptr(engine) as usize;
    LUA_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < LUA_POOL_MAX {
            pool.push(PooledLua { lua, engine_id });
        }
        // else: drop the VM (pool full)
    });
}

// ---------------------------------------------------------------------------
// Script execution entry point
// ---------------------------------------------------------------------------

pub fn execute_script(
    engine: &Arc<CommandEngine>,
    script: &str,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
) -> CommandResponse {
    let lua = take_lua(engine);

    setup_keys_argv(&lua, &keys, &argv);

    let result = match lua.load(script).eval::<LuaValue>() {
        Ok(val) => lua_to_response(val),
        Err(e) => {
            let msg = e.to_string();
            CommandResponse::Error(format!("ERR {}", msg))
        }
    };

    return_lua(lua, engine);
    result
}

// ---------------------------------------------------------------------------
// Sandbox: remove dangerous globals
// ---------------------------------------------------------------------------

fn sandbox_lua(lua: &Lua) {
    let globals = lua.globals();
    let remove = [
        "os", "io", "debug", "loadfile", "dofile", "package", "require",
    ];
    for name in &remove {
        let _ = globals.set(*name, LuaValue::Nil);
    }
}

// ---------------------------------------------------------------------------
// Set up KEYS and ARGV tables
// ---------------------------------------------------------------------------

fn setup_keys_argv(lua: &Lua, keys: &[Bytes], argv: &[Bytes]) {
    fn bytes_to_table(lua: &Lua, items: &[Bytes]) -> LuaTable {
        let t = lua.create_table().expect("create table");
        for (i, item) in items.iter().enumerate() {
            let s = String::from_utf8_lossy(item);
            t.set(i + 1, s.as_ref().to_owned())
                .expect("set table entry");
        }
        t
    }

    let globals = lua.globals();
    globals
        .set("KEYS", bytes_to_table(lua, keys))
        .expect("set KEYS");
    globals
        .set("ARGV", bytes_to_table(lua, argv))
        .expect("set ARGV");
}

// ---------------------------------------------------------------------------
// Set up the redis.* module
// ---------------------------------------------------------------------------

fn setup_redis_module(lua: &Lua, engine: &Arc<CommandEngine>) {
    let redis = lua.create_table().expect("create redis table");

    // redis.call(cmd, ...) — execute command, raise on error
    {
        let eng = engine.clone();
        let call = lua
            .create_function(move |lua, args: LuaMultiValue| {
                let (cmd_name, cmd_args) = parse_redis_call_args(&args)?;
                if is_blocked_in_script(&cmd_name) {
                    return Err(LuaError::RuntimeError(format!(
                        "ERR command '{}' not allowed from script",
                        cmd_name
                    )));
                }
                let resp = eng.execute(&cmd_name, &cmd_args);
                if let CommandResponse::Error(ref msg) = resp {
                    return Err(LuaError::RuntimeError(msg.clone()));
                }
                response_to_lua(lua, resp)
            })
            .expect("create redis.call");
        redis.set("call", call).expect("set redis.call");
    }

    // redis.pcall(cmd, ...) — execute command, return error as table {err=msg}
    {
        let eng = engine.clone();
        let pcall = lua
            .create_function(move |lua, args: LuaMultiValue| {
                let (cmd_name, cmd_args) = parse_redis_call_args(&args)?;
                if is_blocked_in_script(&cmd_name) {
                    let t = lua.create_table()?;
                    t.set(
                        "err",
                        format!("ERR command '{}' not allowed from script", cmd_name),
                    )?;
                    return Ok(LuaValue::Table(t));
                }
                let resp = eng.execute(&cmd_name, &cmd_args);
                if let CommandResponse::Error(ref msg) = resp {
                    let t = lua.create_table()?;
                    t.set("err", msg.clone())?;
                    return Ok(LuaValue::Table(t));
                }
                response_to_lua(lua, resp)
            })
            .expect("create redis.pcall");
        redis.set("pcall", pcall).expect("set redis.pcall");
    }

    // redis.error_reply(msg) — create {err=msg} table
    {
        let error_reply = lua
            .create_function(|lua, msg: String| {
                let t = lua.create_table()?;
                t.set("err", msg)?;
                Ok(LuaValue::Table(t))
            })
            .expect("create redis.error_reply");
        redis
            .set("error_reply", error_reply)
            .expect("set redis.error_reply");
    }

    // redis.status_reply(msg) — create {ok=msg} table
    {
        let status_reply = lua
            .create_function(|lua, msg: String| {
                let t = lua.create_table()?;
                t.set("ok", msg)?;
                Ok(LuaValue::Table(t))
            })
            .expect("create redis.status_reply");
        redis
            .set("status_reply", status_reply)
            .expect("set redis.status_reply");
    }

    // redis.log(level, msg)
    {
        let log_fn = lua
            .create_function(|_lua, (level, msg): (i64, String)| {
                match level {
                    0 => tracing::debug!("[Lua] {}", msg),
                    1 => tracing::debug!("[Lua] {}", msg),
                    2 => tracing::info!("[Lua] {}", msg),
                    _ => tracing::warn!("[Lua] {}", msg),
                }
                Ok(())
            })
            .expect("create redis.log");
        redis.set("log", log_fn).expect("set redis.log");
    }

    // Log level constants
    redis.set("LOG_DEBUG", 0i64).expect("set LOG_DEBUG");
    redis.set("LOG_VERBOSE", 1i64).expect("set LOG_VERBOSE");
    redis.set("LOG_NOTICE", 2i64).expect("set LOG_NOTICE");
    redis.set("LOG_WARNING", 3i64).expect("set LOG_WARNING");

    lua.globals().set("redis", redis).expect("set redis global");
}

// ---------------------------------------------------------------------------
// Parse variadic Lua arguments into (command_name, args)
// ---------------------------------------------------------------------------

fn parse_redis_call_args(args: &LuaMultiValue) -> LuaResult<(String, Vec<Bytes>)> {
    let mut iter = args.iter();
    let cmd_val = iter.next().ok_or_else(|| {
        LuaError::RuntimeError("ERR wrong number of arguments for redis.call/pcall".into())
    })?;

    let cmd_name = match cmd_val {
        LuaValue::String(s) => s.to_str()?.to_uppercase(),
        _ => {
            return Err(LuaError::RuntimeError(
                "ERR first argument to redis.call/pcall must be a string".into(),
            ))
        }
    };

    let mut cmd_args = Vec::new();
    for val in iter {
        match val {
            LuaValue::String(s) => {
                cmd_args.push(Bytes::from(s.as_bytes().to_vec()));
            }
            LuaValue::Integer(n) => {
                cmd_args.push(Bytes::from(n.to_string()));
            }
            LuaValue::Number(n) => {
                // Truncate to integer like Redis does
                cmd_args.push(Bytes::from((*n as i64).to_string()));
            }
            LuaValue::Boolean(b) => {
                cmd_args.push(Bytes::from(if *b { "1" } else { "0" }));
            }
            _ => {
                return Err(LuaError::RuntimeError(
                    "ERR invalid argument type for redis.call/pcall".into(),
                ));
            }
        }
    }

    Ok((cmd_name, cmd_args))
}

// ---------------------------------------------------------------------------
// CommandResponse → Lua value
// ---------------------------------------------------------------------------

fn response_to_lua(lua: &Lua, resp: CommandResponse) -> LuaResult<LuaValue> {
    match resp {
        CommandResponse::Nil => Ok(LuaValue::Boolean(false)),
        CommandResponse::Ok => {
            let t = lua.create_table()?;
            t.set("ok", "OK")?;
            Ok(LuaValue::Table(t))
        }
        CommandResponse::Integer(n) => Ok(LuaValue::Integer(n)),
        CommandResponse::BulkString(b) => {
            let s = lua.create_string(b.as_ref())?;
            Ok(LuaValue::String(s))
        }
        CommandResponse::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", String::from_utf8_lossy(s.as_ref()).to_string())?;
            Ok(LuaValue::Table(t))
        }
        CommandResponse::Array(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                let lua_val = response_to_lua(lua, item)?;
                t.set(i + 1, lua_val)?;
            }
            Ok(LuaValue::Table(t))
        }
        CommandResponse::Error(msg) => Err(LuaError::RuntimeError(msg)),
        CommandResponse::Queued => {
            let t = lua.create_table()?;
            t.set("ok", "QUEUED")?;
            Ok(LuaValue::Table(t))
        }
    }
}

// ---------------------------------------------------------------------------
// Lua value → CommandResponse
// ---------------------------------------------------------------------------

fn lua_to_response(val: LuaValue) -> CommandResponse {
    match val {
        LuaValue::Nil => CommandResponse::Nil,
        LuaValue::Boolean(true) => CommandResponse::Integer(1),
        LuaValue::Boolean(false) => CommandResponse::Nil,
        LuaValue::Integer(n) => CommandResponse::Integer(n),
        LuaValue::Number(n) => CommandResponse::Integer(n as i64),
        LuaValue::String(s) => CommandResponse::BulkString(Bytes::from(s.as_bytes().to_vec())),
        LuaValue::Table(t) => {
            // Check for {err=...}
            if let Ok(err) = t.get::<String>("err") {
                return CommandResponse::Error(err);
            }
            // Check for {ok=...}
            if let Ok(ok) = t.get::<String>("ok") {
                return CommandResponse::SimpleString(Bytes::from(ok));
            }
            // Otherwise treat as array
            let mut items = Vec::new();
            let len = t.len().unwrap_or(0);
            for i in 1..=len {
                if let Ok(v) = t.get::<LuaValue>(i) {
                    items.push(lua_to_response(v));
                } else {
                    break;
                }
            }
            CommandResponse::Array(items)
        }
        _ => CommandResponse::Nil,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_hex() {
        let sha = sha1_hex("return 1");
        assert_eq!(sha.len(), 40);
        // Consistent hash
        assert_eq!(sha, sha1_hex("return 1"));
        // Different inputs produce different hashes
        assert_ne!(sha, sha1_hex("return 2"));
    }

    #[test]
    fn test_script_cache() {
        let cache = ScriptCache::new();

        let sha = cache.load("return 1");
        assert_eq!(sha.len(), 40);
        assert_eq!(cache.get(&sha), Some("return 1".to_owned()));
        assert_eq!(cache.get("nonexistent"), None);

        let sha2 = cache.load("return 2");
        let exists = cache.exists(&[&sha, &sha2, "nonexistent"]);
        assert_eq!(exists, vec![true, true, false]);

        cache.flush();
        assert_eq!(cache.get(&sha), None);
    }

    #[test]
    fn test_blocked_commands() {
        assert!(is_blocked_in_script("EVAL"));
        assert!(is_blocked_in_script("eval"));
        assert!(is_blocked_in_script("MULTI"));
        assert!(is_blocked_in_script("SUBSCRIBE"));
        assert!(!is_blocked_in_script("GET"));
        assert!(!is_blocked_in_script("SET"));
    }

    #[test]
    fn test_lua_to_response_primitives() {
        assert!(matches!(
            lua_to_response(LuaValue::Nil),
            CommandResponse::Nil
        ));
        assert!(matches!(
            lua_to_response(LuaValue::Boolean(true)),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            lua_to_response(LuaValue::Boolean(false)),
            CommandResponse::Nil
        ));
        assert!(matches!(
            lua_to_response(LuaValue::Integer(42)),
            CommandResponse::Integer(42)
        ));
    }

    #[test]
    fn test_lua_to_response_number_truncation() {
        // Lua numbers (floats) should be truncated to integers
        assert!(matches!(
            lua_to_response(LuaValue::Number(3.7)),
            CommandResponse::Integer(3)
        ));
    }

    #[test]
    fn test_response_to_lua_and_back() {
        let lua = Lua::new();

        // Nil -> false -> Nil
        let v = response_to_lua(&lua, CommandResponse::Nil).unwrap();
        assert!(matches!(v, LuaValue::Boolean(false)));
        assert!(matches!(lua_to_response(v), CommandResponse::Nil));

        // Integer round-trips
        let v = response_to_lua(&lua, CommandResponse::Integer(42)).unwrap();
        assert!(matches!(v, LuaValue::Integer(42)));
        assert!(matches!(lua_to_response(v), CommandResponse::Integer(42)));

        // BulkString round-trips
        let v = response_to_lua(&lua, CommandResponse::BulkString(Bytes::from("hello"))).unwrap();
        if let LuaValue::String(s) = &v {
            assert_eq!(s.to_str().unwrap(), "hello");
        } else {
            panic!("expected string");
        }
        match lua_to_response(v) {
            CommandResponse::BulkString(b) => assert_eq!(b.as_ref(), b"hello"),
            other => panic!("expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_sandbox() {
        let lua = Lua::new();
        sandbox_lua(&lua);

        let globals = lua.globals();
        assert!(globals.get::<LuaValue>("os").unwrap().is_nil());
        assert!(globals.get::<LuaValue>("io").unwrap().is_nil());
        assert!(globals.get::<LuaValue>("debug").unwrap().is_nil());
        assert!(globals.get::<LuaValue>("loadfile").unwrap().is_nil());
        assert!(globals.get::<LuaValue>("dofile").unwrap().is_nil());
        assert!(globals.get::<LuaValue>("package").unwrap().is_nil());
        assert!(globals.get::<LuaValue>("require").unwrap().is_nil());

        // Standard safe functions still available
        assert!(!globals.get::<LuaValue>("tostring").unwrap().is_nil());
        assert!(!globals.get::<LuaValue>("tonumber").unwrap().is_nil());
        assert!(!globals.get::<LuaValue>("table").unwrap().is_nil());
        assert!(!globals.get::<LuaValue>("string").unwrap().is_nil());
        assert!(!globals.get::<LuaValue>("math").unwrap().is_nil());
    }

    #[test]
    fn test_execute_script_basic() {
        let shards = Arc::new(crate::ShardManager::new(4));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));

        // Return integer
        let resp = execute_script(&engine, "return 42", vec![], vec![]);
        assert!(matches!(resp, CommandResponse::Integer(42)));

        // Return string
        let resp = execute_script(&engine, "return 'hello'", vec![], vec![]);
        match resp {
            CommandResponse::BulkString(b) => assert_eq!(b.as_ref(), b"hello"),
            other => panic!("expected BulkString, got {:?}", other),
        }

        // Return nil
        let resp = execute_script(&engine, "return nil", vec![], vec![]);
        assert!(matches!(resp, CommandResponse::Nil));

        // Return boolean true → 1
        let resp = execute_script(&engine, "return true", vec![], vec![]);
        assert!(matches!(resp, CommandResponse::Integer(1)));

        // Return boolean false → Nil
        let resp = execute_script(&engine, "return false", vec![], vec![]);
        assert!(matches!(resp, CommandResponse::Nil));
    }

    #[test]
    fn test_execute_script_keys_argv() {
        let shards = Arc::new(crate::ShardManager::new(4));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));

        let resp = execute_script(
            &engine,
            "return KEYS[1]",
            vec![Bytes::from("mykey")],
            vec![],
        );
        match resp {
            CommandResponse::BulkString(b) => assert_eq!(b.as_ref(), b"mykey"),
            other => panic!("expected BulkString, got {:?}", other),
        }

        let resp = execute_script(
            &engine,
            "return ARGV[1]",
            vec![],
            vec![Bytes::from("myarg")],
        );
        match resp {
            CommandResponse::BulkString(b) => assert_eq!(b.as_ref(), b"myarg"),
            other => panic!("expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_script_redis_call() {
        let shards = Arc::new(crate::ShardManager::new(4));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));

        // SET via redis.call
        let resp = execute_script(
            &engine,
            "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])",
            vec![Bytes::from("luakey")],
            vec![Bytes::from("luaval")],
        );
        match resp {
            CommandResponse::BulkString(b) => assert_eq!(b.as_ref(), b"luaval"),
            other => panic!("expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_script_pcall_error() {
        let shards = Arc::new(crate::ShardManager::new(4));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));

        // pcall returns error table instead of raising
        let resp = execute_script(
            &engine,
            "local r = redis.pcall('EVAL', 'return 1', '0') return r.err",
            vec![],
            vec![],
        );
        match resp {
            CommandResponse::BulkString(b) => {
                let s = String::from_utf8_lossy(b.as_ref());
                assert!(s.contains("not allowed from script"), "got: {}", s);
            }
            other => panic!("expected BulkString with error, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_script_table_return() {
        let shards = Arc::new(crate::ShardManager::new(4));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));

        // Return array table
        let resp = execute_script(&engine, "return {1, 2, 3}", vec![], vec![]);
        match resp {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[0], CommandResponse::Integer(1)));
                assert!(matches!(items[1], CommandResponse::Integer(2)));
                assert!(matches!(items[2], CommandResponse::Integer(3)));
            }
            other => panic!("expected Array, got {:?}", other),
        }

        // Return error table
        let resp = execute_script(
            &engine,
            "return redis.error_reply('ERR custom error')",
            vec![],
            vec![],
        );
        match resp {
            CommandResponse::Error(msg) => assert_eq!(msg, "ERR custom error"),
            other => panic!("expected Error, got {:?}", other),
        }

        // Return status table
        let resp = execute_script(&engine, "return redis.status_reply('PONG')", vec![], vec![]);
        match resp {
            CommandResponse::SimpleString(s) => assert_eq!(s.as_ref(), b"PONG"),
            other => panic!("expected SimpleString, got {:?}", other),
        }
    }

    #[test]
    fn test_execute_script_syntax_error() {
        let shards = Arc::new(crate::ShardManager::new(4));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));

        let resp = execute_script(&engine, "invalid lua !!!", vec![], vec![]);
        match resp {
            CommandResponse::Error(msg) => assert!(msg.starts_with("ERR")),
            other => panic!("expected Error, got {:?}", other),
        }
    }
}
