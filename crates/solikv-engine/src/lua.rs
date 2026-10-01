//! Lua scripting: `EVAL`/`EVALSHA` (and their `_RO` forms), `FUNCTION` /
//! `FCALL`, and the `redis.*` API scripts call.
//!
//! # Compatibility
//!
//! Scripts written for Redis assume Redis' Lua: **Lua 5.1** (numbers are
//! doubles, `unpack` is a global, `tostring(3)` is `"3"`), with `cjson`,
//! `cmsgpack`, `bit` and `struct` loaded ([`crate::lua_libs`]). Values cross
//! the boundary with Redis' conversion rules: a Lua number becomes an
//! integer reply (truncated), `false` a nil, `{ok=…}` a status, `{err=…}` an
//! error; a nil reply becomes `false`, a status `{ok=…}`.
//!
//! # Isolation (Redis 7's read-only globals)
//!
//! A script runs with an environment that is an empty proxy: reading a
//! global looks it up in a view of the allowed globals (and raises
//! "Script attempted to access nonexistent global variable" for anything
//! else), writing one raises "Attempt to modify a readonly table". The
//! libraries (`string`, `table`, `math`, `redis`, `cjson`, …) are read-only
//! proxies of the real tables too, so `string.rep = nil` fails instead of
//! changing the next script's `string`. The real tables are unreachable:
//! `getfenv`/`setfenv`/`debug` are not exposed and the string metatable is
//! protected. What `rawset` can still put into a proxy is wiped after the
//! script (or the VM discarded).
//!
//! # Long scripts
//!
//! A script holds every shard for its duration, as Redis' single thread
//! would. Once it has run for `busy-reply-threshold` (5 s), other clients
//! get `-BUSY` instead of queueing behind it, and `SCRIPT KILL` (a script
//! that has not written) or `FUNCTION KILL` stops it: an instruction hook
//! sees the flag and raises an error that the script's own `pcall` cannot
//! swallow.

use bytes::Bytes;
use dashmap::DashMap;
use mlua::prelude::*;
use parking_lot::RwLock;
use sha1::{Digest, Sha1};
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// SHA1 → source, shared: `EVALSHA` takes a reference, not a copy.
    scripts: DashMap<String, Arc<str>>,
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
        self.scripts.insert(sha.clone(), Arc::from(source));
        sha
    }

    /// Retrieve a cached script by SHA1.
    pub fn get(&self, sha: &str) -> Option<Arc<str>> {
        self.scripts.get(sha).map(|r| r.value().clone())
    }

    /// Check existence of one or more SHA1 digests. Returns vec of booleans.
    pub fn exists(&self, shas: &[&str]) -> Vec<bool> {
        shas.iter()
            .map(|s| self.scripts.contains_key(&s.to_ascii_lowercase()))
            .collect()
    }

    /// Clear the entire cache.
    pub fn flush(&self) {
        self.scripts.clear();
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scripts.is_empty()
    }
}

// ---------------------------------------------------------------------------
// SHA1 helper
// ---------------------------------------------------------------------------

pub fn sha1_hex(input: &str) -> String {
    sha1_hex_bytes(input.as_bytes())
}

pub fn sha1_hex_bytes(input: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input);
    format!("{:x}", hasher.finalize())
}

// ---------------------------------------------------------------------------
// Script flags (`#!lua flags=…`) and function flags
// ---------------------------------------------------------------------------

/// Flags declared by a script's shebang or a function's registration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScriptFlags {
    pub no_writes: bool,
    pub allow_oom: bool,
    pub allow_stale: bool,
    pub no_cluster: bool,
    pub allow_cross_slot_keys: bool,
    /// The script had a shebang (new-style semantics).
    pub shebang: bool,
}

impl ScriptFlags {
    fn set(&mut self, flag: &str) -> Result<(), String> {
        match flag {
            "no-writes" => self.no_writes = true,
            "allow-oom" => self.allow_oom = true,
            "allow-stale" => self.allow_stale = true,
            "no-cluster" => self.no_cluster = true,
            "allow-cross-slot-keys" => self.allow_cross_slot_keys = true,
            _ => return Err(flag.to_string()),
        }
        Ok(())
    }

    pub fn names(&self) -> Vec<&'static str> {
        let mut v = Vec::new();
        if self.no_writes {
            v.push("no-writes");
        }
        if self.allow_oom {
            v.push("allow-oom");
        }
        if self.allow_stale {
            v.push("allow-stale");
        }
        if self.no_cluster {
            v.push("no-cluster");
        }
        if self.allow_cross_slot_keys {
            v.push("allow-cross-slot-keys");
        }
        v
    }
}

/// Split an `EVAL` script into its flags and the Lua to compile. A shebang
/// line (`#!lua flags=no-writes,allow-oom`) is replaced by an empty line so
/// error line numbers still match the source.
pub fn parse_script(src: &str) -> Result<(ScriptFlags, Cow<'_, str>), String> {
    if !src.starts_with("#!") {
        return Ok((ScriptFlags::default(), Cow::Borrowed(src)));
    }
    let (line, rest) = match src.find('\n') {
        Some(i) => (&src[2..i], &src[i..]),
        None => (&src[2..], ""),
    };
    let mut parts = line.split_ascii_whitespace();
    let engine = parts.next().unwrap_or("");
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Could not find engine '{}'", engine));
    }
    let mut flags = ScriptFlags {
        shebang: true,
        ..Default::default()
    };
    for part in parts {
        let Some(list) = part.strip_prefix("flags=") else {
            return Err(format!("ERR Unknown lua shebang option: {}", part));
        };
        for f in list.split(',').filter(|f| !f.is_empty()) {
            flags
                .set(f)
                .map_err(|f| format!("ERR Unexpected flag in script shebang: {}", f))?;
        }
    }
    Ok((flags, Cow::Owned(rest.to_string())))
}

// ---------------------------------------------------------------------------
// The script running now (BUSY, SCRIPT KILL)
// ---------------------------------------------------------------------------

/// State of the script or function currently running. One at a time: a
/// script holds every shard.
#[derive(Default)]
pub struct RunState {
    running: AtomicBool,
    started_ms: AtomicU64,
    kill: AtomicBool,
    wrote: AtomicBool,
    function: AtomicBool,
    name: parking_lot::Mutex<(String, String)>,
    done: tokio::sync::Notify,
}

/// Error a killed script ends with.
const KILLED: &str = "ERR Script killed by user with SCRIPT KILL...";
const FN_KILLED: &str = "ERR Script killed by user with FUNCTION KILL...";

impl RunState {
    #[inline]
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn is_function(&self) -> bool {
        self.function.load(Ordering::Relaxed)
    }

    pub fn elapsed_ms(&self) -> u64 {
        now_ms().saturating_sub(self.started_ms.load(Ordering::Relaxed))
    }

    /// `(name, command)` of what runs, for `FUNCTION STATS`.
    pub fn describe(&self) -> (String, String) {
        self.name.lock().clone()
    }

    pub(crate) fn enter(&self, function: bool, name: &str, cmd: &str) -> RunGuard<'_> {
        self.kill.store(false, Ordering::Relaxed);
        self.wrote.store(false, Ordering::Relaxed);
        self.function.store(function, Ordering::Relaxed);
        *self.name.lock() = (name.to_string(), cmd.to_string());
        self.started_ms.store(now_ms(), Ordering::Relaxed);
        self.running.store(true, Ordering::Release);
        RunGuard(self)
    }

    /// `SCRIPT KILL` / `FUNCTION KILL`.
    pub fn kill(&self, function_kill: bool) -> CommandResponse {
        if !self.is_running() {
            return CommandResponse::error("NOTBUSY No scripts in execution right now.");
        }
        if self.is_function() != function_kill {
            return CommandResponse::error(if self.is_function() {
                "BUSY Redis is busy running a function. You can only call FUNCTION KILL or \
                 SHUTDOWN NOSAVE."
            } else {
                "BUSY Redis is busy running a script. You can only call SCRIPT KILL or \
                 SHUTDOWN NOSAVE."
            });
        }
        if self.wrote.load(Ordering::Relaxed) {
            return CommandResponse::error(
                "UNKILLABLE Sorry the script already executed write commands against the \
                 dataset. You can either wait the script termination or kill the server in a \
                 hard way using the SHUTDOWN NOSAVE command.",
            );
        }
        self.kill.store(true, Ordering::Release);
        CommandResponse::ok()
    }

    pub(crate) fn note_write(&self) {
        self.wrote.store(true, Ordering::Relaxed);
    }

    fn killed(&self) -> bool {
        self.kill.load(Ordering::Acquire)
    }

    /// Resolves when the running script ends (or at once if none runs).
    pub async fn wait_done(&self, max: std::time::Duration) {
        let notified = self.done.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_running() {
            return;
        }
        let _ = tokio::time::timeout(max, notified).await;
    }
}

pub(crate) struct RunGuard<'a>(&'a RunState);

impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
        self.0.kill.store(false, Ordering::Relaxed);
        self.0.done.notify_waiters();
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Functions (FUNCTION LOAD / FCALL)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: String,
    pub library: String,
    pub flags: ScriptFlags,
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Library {
    pub name: String,
    pub code: String,
    pub functions: Vec<FunctionMeta>,
}

#[derive(Default)]
struct FnInner {
    libs: BTreeMap<String, Library>,
    fns: HashMap<String, FunctionMeta>,
}

/// Loaded function libraries. Each pooled VM loads them lazily and reloads
/// when [`version`](Self::version) moves.
#[derive(Default)]
pub struct FunctionRegistry {
    inner: RwLock<FnInner>,
    version: AtomicU64,
}

/// Serialized libraries (`FUNCTION DUMP`, the functions file).
const FN_DUMP_MAGIC: &[u8] = b"SOLIKVFN1";

impl FunctionRegistry {
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn function(&self, name: &str) -> Option<FunctionMeta> {
        self.inner.read().fns.get(name).cloned()
    }

    pub fn libraries(&self) -> Vec<Library> {
        self.inner.read().libs.values().cloned().collect()
    }

    pub fn counts(&self) -> (usize, usize) {
        let g = self.inner.read();
        (g.libs.len(), g.fns.len())
    }

    /// Validate and add `lib` (already loaded once to learn its functions).
    fn install(&self, lib: Library, replace: bool) -> Result<(), String> {
        let mut g = self.inner.write();
        if g.libs.contains_key(&lib.name) && !replace {
            return Err(format!("ERR Library '{}' already exists", lib.name));
        }
        for f in &lib.functions {
            if let Some(existing) = g.fns.get(&f.name) {
                if existing.library != lib.name {
                    return Err(format!("ERR Function {} already exists", f.name));
                }
            }
        }
        if let Some(old) = g.libs.remove(&lib.name) {
            for f in old.functions {
                g.fns.remove(&f.name);
            }
        }
        for f in &lib.functions {
            g.fns.insert(f.name.clone(), f.clone());
        }
        g.libs.insert(lib.name.clone(), lib);
        drop(g);
        self.version.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn delete(&self, name: &str) -> bool {
        let mut g = self.inner.write();
        let Some(old) = g.libs.remove(name) else {
            return false;
        };
        for f in old.functions {
            g.fns.remove(&f.name);
        }
        drop(g);
        self.version.fetch_add(1, Ordering::AcqRel);
        true
    }

    pub fn flush(&self) {
        let mut g = self.inner.write();
        g.libs.clear();
        g.fns.clear();
        drop(g);
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    /// Every library's code, serialized.
    pub fn dump(&self) -> Vec<u8> {
        let g = self.inner.read();
        let mut out = FN_DUMP_MAGIC.to_vec();
        out.extend_from_slice(&(g.libs.len() as u32).to_le_bytes());
        for lib in g.libs.values() {
            out.extend_from_slice(&(lib.code.len() as u32).to_le_bytes());
            out.extend_from_slice(lib.code.as_bytes());
        }
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }
}

/// Library sources in a `dump`.
pub fn parse_function_dump(payload: &[u8]) -> Result<Vec<String>, String> {
    let bad = || "ERR payload version or checksum are wrong".to_string();
    if payload.len() < FN_DUMP_MAGIC.len() + 8 || !payload.starts_with(FN_DUMP_MAGIC) {
        return Err(bad());
    }
    let (body, crc) = payload.split_at(payload.len() - 4);
    if crc32(body).to_le_bytes() != crc {
        return Err(bad());
    }
    let mut i = FN_DUMP_MAGIC.len();
    let count = u32::from_le_bytes(body[i..i + 4].try_into().map_err(|_| bad())?) as usize;
    i += 4;
    let mut out = Vec::new();
    for _ in 0..count {
        let len = u32::from_le_bytes(
            body.get(i..i + 4)
                .ok_or_else(bad)?
                .try_into()
                .map_err(|_| bad())?,
        ) as usize;
        i += 4;
        let code = body.get(i..i + len).ok_or_else(bad)?;
        i += len;
        out.push(String::from_utf8(code.to_vec()).map_err(|_| bad())?);
    }
    Ok(out)
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn valid_function_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// `#!lua name=<lib>` → library name, and the code with the shebang blanked.
fn parse_library_header(code: &str) -> Result<String, String> {
    if !code.starts_with("#!") {
        return Err("ERR Missing library metadata".into());
    }
    let line = code[2..].lines().next().unwrap_or("");
    let mut parts = line.split_ascii_whitespace();
    let engine = parts.next().unwrap_or("");
    if !engine.eq_ignore_ascii_case("lua") {
        return Err(format!("ERR Engine '{}' not found", engine));
    }
    let mut name = None;
    for p in parts {
        match p.split_once('=') {
            Some(("name", v)) => name = Some(v.to_string()),
            _ => return Err(format!("ERR Invalid metadata value given: {}", p)),
        }
    }
    let name = name.ok_or_else(|| "ERR Library name was not given".to_string())?;
    if !valid_function_name(&name) {
        return Err(
            "ERR Library names can only contain letters, numbers, or underscores(_) and must \
             be at least one character long"
                .into(),
        );
    }
    Ok(name)
}

// ---------------------------------------------------------------------------
// Per-run context (thread-local: a script runs on one thread)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct RunCtx {
    /// Write commands refused (`EVAL_RO`, `FCALL_RO`, `no-writes`).
    read_only: bool,
    /// Protocol the script sees replies in (`redis.setresp`).
    resp: u8,
    /// Loading a function library: only `register_function` is usable.
    loading: bool,
}

/// A registration collected while a library loads.
struct Registered {
    name: String,
    callback: Option<LuaFunction>,
    flags: ScriptFlags,
    description: Option<String>,
}

thread_local! {
    static RUN: Cell<RunCtx> = const { Cell::new(RunCtx { read_only: false, resp: 2, loading: false }) };
    static REGISTERED: RefCell<Vec<Registered>> = const { RefCell::new(Vec::new()) };
    static LOAD_DEADLINE: Cell<Option<std::time::Instant>> = const { Cell::new(None) };
}

fn run_ctx() -> RunCtx {
    RUN.with(|r| r.get())
}

fn set_run_ctx(ctx: RunCtx) -> RunCtx {
    RUN.with(|r| r.replace(ctx))
}

// ---------------------------------------------------------------------------
// Thread-local Lua VM pool
// ---------------------------------------------------------------------------

/// A pooled Lua VM tagged with its owning engine's identity.
struct PooledLua {
    lua: Lua,
    engine_id: usize,
    /// The allowed globals (read through `env`). `KEYS`/`ARGV` are set here.
    view: LuaTable,
    /// The environment scripts run in: an empty read-only proxy of `view`.
    env: LuaTable,
    /// Read-only proxies of the libraries; must stay empty.
    proxies: Vec<LuaTable>,
    /// `pcall(f, …)` in Lua, turning any error into `(false, kind, msg)`.
    runner: LuaFunction,
    /// Compiled scripts, by source: `EVAL`/`EVALSHA` of a script this VM ran
    /// before calls the function instead of parsing and compiling it again.
    compiled: HashMap<String, LuaFunction, solikv_core::FastHasher>,
    /// Function libraries loaded into this VM, at registry version `fn_version`.
    fn_version: u64,
    fns: HashMap<String, LuaFunction>,
}

/// Most compiled scripts one VM keeps (then it starts over).
const COMPILED_MAX: usize = 512;

thread_local! {
    static LUA_POOL: RefCell<Vec<PooledLua>> = const { RefCell::new(Vec::new()) };
}

/// Take a pre-sandboxed Lua VM from the thread-local pool, or create one.
fn take_lua(engine: &Arc<CommandEngine>) -> LuaResult<PooledLua> {
    let engine_id = Arc::as_ptr(engine) as usize;
    let cached = LUA_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        pool.iter()
            .rposition(|p| p.engine_id == engine_id)
            .map(|idx| pool.swap_remove(idx))
    });
    if let Some(vm) = cached {
        return Ok(vm);
    }
    new_vm(engine)
}

/// Globals a script may read, as Redis 7 allows them.
const ALLOWED_GLOBALS: &[&str] = &[
    "assert",
    "error",
    "ipairs",
    "next",
    "pairs",
    "rawequal",
    "rawget",
    "rawset",
    "select",
    "setmetatable",
    "getmetatable",
    "tonumber",
    "tostring",
    "type",
    "unpack",
    "gcinfo",
    "_VERSION",
];

/// Libraries exposed read-only.
const ALLOWED_LIBS: &[&str] = &["string", "table", "math", "coroutine"];

fn readonly_proxy(lua: &Lua, target: &LuaTable) -> LuaResult<LuaTable> {
    let proxy = lua.create_table()?;
    let meta = lua.create_table()?;
    meta.set("__index", target.clone())?;
    meta.set(
        "__newindex",
        lua.create_function(|_, _: LuaMultiValue| -> LuaResult<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".into(),
            ))
        })?,
    )?;
    meta.set("__metatable", false)?;
    proxy.set_metatable(Some(meta));
    Ok(proxy)
}

fn new_vm(engine: &Arc<CommandEngine>) -> LuaResult<PooledLua> {
    let lua = Lua::new();
    let globals = lua.globals();
    let view = lua.create_table()?;
    let mut proxies = Vec::new();

    for name in ALLOWED_GLOBALS {
        let v: LuaValue = globals.raw_get(*name)?;
        view.raw_set(*name, v)?;
    }
    for name in ALLOWED_LIBS {
        let t: LuaTable = globals.raw_get(*name)?;
        let p = readonly_proxy(&lua, &t)?;
        view.raw_set(*name, p.clone())?;
        proxies.push(p);
    }
    // `os.clock` only, as in Redis.
    {
        let os_real: LuaTable = globals.raw_get("os")?;
        let os = lua.create_table()?;
        os.raw_set("clock", os_real.raw_get::<LuaValue>("clock")?)?;
        let p = readonly_proxy(&lua, &os)?;
        view.raw_set("os", p.clone())?;
        proxies.push(p);
    }
    for (name, t) in crate::lua_libs::install(&lua)? {
        let p = readonly_proxy(&lua, &t)?;
        view.raw_set(name, p.clone())?;
        proxies.push(p);
    }
    let redis = setup_redis_module(&lua, engine)?;
    let redis_proxy = readonly_proxy(&lua, &redis)?;
    view.raw_set("redis", redis_proxy.clone())?;
    proxies.push(redis_proxy);

    // `pcall`/`xpcall` that do not swallow SCRIPT KILL.
    let state = engine.script_state().clone();
    let killed = lua.create_function(move |_, ()| Ok(state.killed()))?;
    let wrap: LuaTable = lua
        .load(
            r#"
            local pcall, xpcall, select, unpack, error, killed = ...
            local function pack(...) return { n = select('#', ...), ... } end
            local function check(r)
                if not r[1] and killed() then error(r[2], 0) end
                return unpack(r, 1, r.n)
            end
            return {
                pcall = function(...) return check(pack(pcall(...))) end,
                xpcall = function(f, h) return check(pack(xpcall(f, h))) end,
            }
            "#,
        )
        .set_name("=wrappers")
        .call((
            globals.raw_get::<LuaValue>("pcall")?,
            globals.raw_get::<LuaValue>("xpcall")?,
            globals.raw_get::<LuaValue>("select")?,
            globals.raw_get::<LuaValue>("unpack")?,
            globals.raw_get::<LuaValue>("error")?,
            killed,
        ))?;
    view.raw_set("pcall", wrap.raw_get::<LuaValue>("pcall")?)?;
    view.raw_set("xpcall", wrap.raw_get::<LuaValue>("xpcall")?)?;

    // The environment: `_G` is the proxy itself; unknown names raise.
    let env = lua.create_table()?;
    view.raw_set("_G", env.clone())?;
    let view_meta = lua.create_table()?;
    view_meta.set(
        "__index",
        lua.create_function(|_, (_, k): (LuaValue, LuaValue)| -> LuaResult<LuaValue> {
            let name = match &k {
                LuaValue::String(s) => s.to_string_lossy().to_string(),
                other => format!("{:?}", other),
            };
            Err(LuaError::RuntimeError(format!(
                "Script attempted to access nonexistent global variable '{}'",
                name
            )))
        })?,
    )?;
    view.set_metatable(Some(view_meta));
    let env_meta = lua.create_table()?;
    env_meta.set("__index", view.clone())?;
    env_meta.set(
        "__newindex",
        lua.create_function(|_, _: LuaMultiValue| -> LuaResult<()> {
            Err(LuaError::RuntimeError(
                "Attempt to modify a readonly table".into(),
            ))
        })?,
    )?;
    env_meta.set("__metatable", false)?;
    env.set_metatable(Some(env_meta));

    // The string metatable (`("x"):upper()`) reaches the real `string`
    // table: protect it, or `getmetatable("").__index.upper = …` would
    // change every later script's strings.
    lua.load(r#"local mt = getmetatable(""); mt.__metatable = false"#)
        .exec()?;

    let runner: LuaFunction = lua
        .load(
            r#"
            local pcall, type, tostring, rawget = pcall, type, tostring, rawget
            return function(f, ...)
                local ok, r = pcall(f, ...)
                if ok then return true, r end
                if type(r) == 'table' then
                    local e = rawget(r, 'err')
                    if type(e) == 'string' then return false, 'T', e end
                    return false, 'S', 'Unknown error'
                end
                return false, 'S', tostring(r)
            end
            "#,
        )
        .set_name("=runner")
        .call(())?;

    // SCRIPT KILL / FUNCTION KILL, and a library load's time limit.
    let state = engine.script_state().clone();
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(10_000),
        move |_lua, _debug| {
            if state.killed() {
                return Err(LuaError::RuntimeError(
                    if state.is_function() {
                        FN_KILLED
                    } else {
                        KILLED
                    }
                    .into(),
                ));
            }
            if LOAD_DEADLINE
                .with(|d| d.get())
                .is_some_and(|d| std::time::Instant::now() > d)
            {
                return Err(LuaError::RuntimeError("ERR FUNCTION LOAD timeout".into()));
            }
            Ok(mlua::VmState::Continue)
        },
    );

    // After setup, so the limit bounds what scripts allocate, not the VM's
    // own tables.
    let limit = engine.lua_memory_limit.max(lua.used_memory() + 1024 * 1024);
    if let Err(e) = lua.set_memory_limit(limit) {
        tracing::warn!("could not set the Lua memory limit: {}", e);
    }

    Ok(PooledLua {
        lua,
        engine_id: Arc::as_ptr(engine) as usize,
        view,
        env,
        proxies,
        runner,
        compiled: Default::default(),
        fn_version: u64::MAX,
        fns: HashMap::new(),
    })
}

/// Whether a script left the VM as it found it: nothing written into the
/// environment or a library proxy (`rawset` goes around their guards).
fn vm_intact(vm: &PooledLua) -> bool {
    let empty = |t: &LuaTable| t.clone().pairs::<LuaValue, LuaValue>().next().is_none();
    empty(&vm.env) && vm.proxies.iter().all(empty)
}

/// Return a Lua VM to the thread-local pool.
fn return_lua(mut vm: PooledLua, engine: &Arc<CommandEngine>) {
    if !vm_intact(&vm) {
        tracing::warn!("a script wrote around the read-only globals; its Lua VM is discarded");
        return;
    }
    let _ = vm.view.raw_set("KEYS", LuaValue::Nil);
    let _ = vm.view.raw_set("ARGV", LuaValue::Nil);
    // Free what the script left behind, and retire a VM that is still close
    // to its memory limit: reusing it could fail to allocate.
    let _ = vm.lua.gc_collect();
    if vm.lua.used_memory() > engine.lua_memory_limit / 2 {
        return;
    }
    if vm.compiled.len() > COMPILED_MAX {
        vm.compiled.clear();
    }
    LUA_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < LUA_POOL_MAX {
            pool.push(vm);
        }
    });
}

// ---------------------------------------------------------------------------
// Script execution entry points
// ---------------------------------------------------------------------------

/// Run `script` (an `EVAL` body, shebang already parsed) with writes allowed.
pub fn execute_script(
    engine: &Arc<CommandEngine>,
    script: &str,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
) -> CommandResponse {
    execute_script_with(engine, script, keys, argv, false)
}

/// Run an `EVAL` body. `read_only`: `EVAL_RO` or a `no-writes` script.
pub fn execute_script_with(
    engine: &Arc<CommandEngine>,
    script: &str,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    read_only: bool,
) -> CommandResponse {
    let mut vm = match take_lua(engine) {
        Ok(vm) => vm,
        Err(e) => return CommandResponse::Error(format!("ERR {}", e)),
    };
    // Text only. A precompiled (binary) chunk is not verified by Lua and is
    // a known way to corrupt the VM's memory from inside a script.
    let func = match vm.compiled.get(script) {
        Some(f) => Ok(f.clone()),
        None => vm
            .lua
            .load(script)
            .set_name("@user_script")
            .set_mode(mlua::ChunkMode::Text)
            .set_environment(vm.env.clone())
            .into_function()
            .inspect(|f| {
                vm.compiled.insert(script.to_owned(), f.clone());
            }),
    };
    let result = match func {
        Err(e) => CommandResponse::Error(format!(
            "ERR Error compiling script (new function): {}",
            clean_error(&e.to_string())
        )),
        Ok(f) => {
            let prev = set_run_ctx(RunCtx {
                read_only,
                resp: 2,
                loading: false,
            });
            let r = setup_keys_argv(&vm, &keys, &argv)
                .and_then(|()| vm.runner.call::<LuaMultiValue>(f).map(runner_result));
            set_run_ctx(prev);
            r.unwrap_or_else(|e| {
                CommandResponse::Error(format!("ERR {}", clean_error(&e.to_string())))
            })
        }
    };
    return_lua(vm, engine);
    result
}

/// Run function `name` (`FCALL`), its library loaded into this thread's VM.
pub fn execute_function(
    engine: &Arc<CommandEngine>,
    name: &str,
    keys: Vec<Bytes>,
    argv: Vec<Bytes>,
    read_only: bool,
) -> CommandResponse {
    let mut vm = match take_lua(engine) {
        Ok(vm) => vm,
        Err(e) => return CommandResponse::Error(format!("ERR {}", e)),
    };
    let registry = engine.functions();
    let version = registry.version();
    if vm.fn_version != version {
        vm.fns.clear();
        for lib in registry.libraries() {
            match load_library_into(&vm, &lib.code) {
                Ok(regs) => {
                    for r in regs {
                        if let Some(cb) = r.callback {
                            vm.fns.insert(r.name, cb);
                        }
                    }
                }
                Err(e) => tracing::warn!("function library {} failed to load: {}", lib.name, e),
            }
        }
        vm.fn_version = version;
    }
    let result = match vm.fns.get(name).cloned() {
        None => CommandResponse::error("ERR Function not found"),
        Some(f) => {
            let prev = set_run_ctx(RunCtx {
                read_only,
                resp: 2,
                loading: false,
            });
            let r = (|| -> LuaResult<CommandResponse> {
                let k = bytes_table(&vm.lua, &keys)?;
                let a = bytes_table(&vm.lua, &argv)?;
                let out = vm.runner.call::<LuaMultiValue>((f, k, a))?;
                Ok(runner_result(out))
            })();
            set_run_ctx(prev);
            r.unwrap_or_else(|e| {
                CommandResponse::Error(format!("ERR {}", clean_error(&e.to_string())))
            })
        }
    };
    return_lua(vm, engine);
    result
}

/// Load a library's code in a throwaway VM to validate it and learn its
/// functions (`FUNCTION LOAD`), then add it to the registry.
pub fn load_library(
    engine: &Arc<CommandEngine>,
    code: &str,
    replace: bool,
) -> Result<String, String> {
    let name = parse_library_header(code)?;
    let vm = new_vm(engine).map_err(|e| format!("ERR {}", e))?;
    let regs = load_library_into(&vm, code)?;
    if regs.is_empty() {
        return Err("ERR No functions registered".into());
    }
    let lib = Library {
        name: name.clone(),
        code: code.to_string(),
        functions: regs
            .into_iter()
            .map(|r| FunctionMeta {
                name: r.name,
                library: name.clone(),
                flags: r.flags,
                description: r.description,
            })
            .collect(),
    };
    engine.functions().install(lib, replace)?;
    Ok(name)
}

/// Run a library's body in `vm` with `register_function` enabled, returning
/// what it registered.
fn load_library_into(vm: &PooledLua, code: &str) -> Result<Vec<Registered>, String> {
    // The shebang is not Lua: blank it, keeping line numbers.
    let body = match code.find('\n') {
        Some(i) => &code[i..],
        None => "",
    };
    let f = vm
        .lua
        .load(body)
        .set_name("@user_function")
        .set_mode(mlua::ChunkMode::Text)
        .set_environment(vm.env.clone())
        .into_function()
        .map_err(|e| {
            format!(
                "ERR Error compiling function: {}",
                clean_error(&e.to_string())
            )
        })?;
    REGISTERED.with(|r| r.borrow_mut().clear());
    let prev = set_run_ctx(RunCtx {
        read_only: true,
        resp: 2,
        loading: true,
    });
    LOAD_DEADLINE.with(|d| {
        d.set(Some(
            std::time::Instant::now() + std::time::Duration::from_millis(500),
        ))
    });
    let out = vm.runner.call::<LuaMultiValue>(f);
    LOAD_DEADLINE.with(|d| d.set(None));
    set_run_ctx(prev);
    let regs = REGISTERED.with(|r| std::mem::take(&mut *r.borrow_mut()));
    match out {
        Err(e) => Err(format!(
            "ERR Error registering functions: {}",
            clean_error(&e.to_string())
        )),
        Ok(out) => match runner_result(out) {
            CommandResponse::Error(e) => Err(format!("ERR Error registering functions: {}", e)),
            _ => Ok(regs),
        },
    }
}

/// `(true, value)` or `(false, kind, message)` from the runner.
fn runner_result(out: LuaMultiValue) -> CommandResponse {
    let mut it = out.into_iter();
    match it.next() {
        Some(LuaValue::Boolean(true)) => lua_to_response(it.next().unwrap_or(LuaValue::Nil)),
        _ => {
            let kind = it.next();
            let msg = match it.next() {
                Some(LuaValue::String(s)) => s.to_string_lossy().to_string(),
                _ => "Unknown error".to_string(),
            };
            match kind {
                // An error reply raised by `redis.call` or returned as
                // `{err=…}`: the reply is that error, as it was.
                Some(LuaValue::String(k)) if k.as_bytes().as_ref() == b"T" => {
                    CommandResponse::Error(msg.strip_prefix('-').unwrap_or(&msg).to_string())
                }
                _ => {
                    let msg = clean_error(&msg);
                    if msg.starts_with("ERR ") {
                        CommandResponse::Error(msg)
                    } else {
                        CommandResponse::Error(format!("ERR {}", msg))
                    }
                }
            }
        }
    }
}

/// An error message without mlua's decorations ("runtime error: ",
/// "callback error", the traceback).
fn clean_error(msg: &str) -> String {
    let msg = match msg.rfind("runtime error: ") {
        Some(i) => &msg[i + "runtime error: ".len()..],
        None => msg,
    };
    let msg = msg.strip_prefix("syntax error: ").unwrap_or(msg);
    let first = msg.lines().next().unwrap_or(msg);
    first.trim_end().to_string()
}

fn bytes_table(lua: &Lua, items: &[Bytes]) -> LuaResult<LuaTable> {
    let t = lua.create_table_with_capacity(items.len(), 0)?;
    for (i, item) in items.iter().enumerate() {
        t.raw_set(i + 1, lua.create_string(item.as_ref())?)?;
    }
    Ok(t)
}

/// Fallible: under the memory limit, building the tables can fail, and that
/// is an error for this script — not a panic in the server.
fn setup_keys_argv(vm: &PooledLua, keys: &[Bytes], argv: &[Bytes]) -> LuaResult<()> {
    vm.view.raw_set("KEYS", bytes_table(&vm.lua, keys)?)?;
    vm.view.raw_set("ARGV", bytes_table(&vm.lua, argv)?)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The redis.* module
// ---------------------------------------------------------------------------

fn err_table(lua: &Lua, msg: impl Into<String>) -> LuaResult<LuaValue> {
    let t = lua.create_table()?;
    t.raw_set("err", msg.into())?;
    Ok(LuaValue::Table(t))
}

/// Run one `redis.call`/`redis.pcall`: the reply, or an `{err=…}` table.
fn redis_call(lua: &Lua, engine: &Arc<CommandEngine>, args: LuaMultiValue) -> LuaResult<LuaValue> {
    let ctx = run_ctx();
    if ctx.loading {
        return err_table(
            lua,
            "ERR redis.call can only be called inside a script invocation",
        );
    }
    let (name, cmd_args) = match parse_redis_call_args(&args) {
        Ok(v) => v,
        Err(e) => return err_table(lua, e),
    };
    let Some(spec) = crate::commands::lookup(&name) else {
        return err_table(lua, "ERR Unknown Redis command called from script");
    };
    if spec.has(crate::commands::F_NOSCRIPT) {
        return err_table(lua, "ERR This Redis command is not allowed from script");
    }
    if !spec.arity_ok(cmd_args.len()) {
        return err_table(
            lua,
            "ERR Wrong number of args calling Redis command from script",
        );
    }
    if spec.is_write() {
        if ctx.read_only {
            return err_table(
                lua,
                "ERR Write commands are not allowed from read-only scripts.",
            );
        }
        engine.script_state().note_write();
    }
    if let Err(e) = engine.script_acl_check(spec, &cmd_args) {
        return err_table(lua, e);
    }
    engine.feed_monitor_script(&name, &cmd_args);
    let resp = engine.execute(spec.name, &cmd_args);
    if let CommandResponse::Error(msg) = resp {
        return err_table(lua, msg);
    }
    response_to_lua(lua, resp, ctx.resp)
}

/// Install `redis.*`; returns the real table (callers expose a read-only
/// proxy of it).
fn setup_redis_module(lua: &Lua, engine: &Arc<CommandEngine>) -> LuaResult<LuaTable> {
    let redis = lua.create_table()?;

    let eng = engine.clone();
    let rpcall =
        lua.create_function(move |lua, args: LuaMultiValue| redis_call(lua, &eng, args))?;
    redis.raw_set("pcall", rpcall.clone())?;
    // `redis.call`: the same, raising the error table (Redis 7 raises a
    // table with `err`, which `pcall` hands back as such).
    let call: LuaFunction = lua
        .load(
            r#"
            local rpcall, error, type, rawget = ...
            return function(...)
                local r = rpcall(...)
                if type(r) == 'table' and rawget(r, 'err') ~= nil then error(r) end
                return r
            end
            "#,
        )
        .set_name("=redis.call")
        .call((
            rpcall,
            lua.globals().raw_get::<LuaValue>("error")?,
            lua.globals().raw_get::<LuaValue>("type")?,
            lua.globals().raw_get::<LuaValue>("rawget")?,
        ))?;
    redis.raw_set("call", call)?;

    redis.raw_set(
        "error_reply",
        lua.create_function(|lua, msg: LuaString| {
            let t = lua.create_table()?;
            t.raw_set("err", msg)?;
            Ok(LuaValue::Table(t))
        })?,
    )?;
    redis.raw_set(
        "status_reply",
        lua.create_function(|lua, msg: LuaString| {
            let t = lua.create_table()?;
            t.raw_set("ok", msg)?;
            Ok(LuaValue::Table(t))
        })?,
    )?;
    redis.raw_set(
        "log",
        lua.create_function(|_lua, args: LuaMultiValue| {
            let mut it = args.into_iter();
            let level = match it.next() {
                Some(LuaValue::Integer(l)) => l,
                Some(LuaValue::Number(l)) => l as i64,
                _ => {
                    return Err(LuaError::RuntimeError(
                        "First argument must be a number (log level).".into(),
                    ))
                }
            };
            let msg = it
                .map(|v| match v {
                    LuaValue::String(s) => s.to_string_lossy().to_string(),
                    LuaValue::Integer(i) => i.to_string(),
                    LuaValue::Number(n) => crate::lua_libs::format_g(n, 14),
                    other => format!("{:?}", other),
                })
                .collect::<Vec<_>>()
                .join(" ");
            match level {
                0 | 1 => tracing::debug!("[Lua] {}", msg),
                2 => tracing::info!("[Lua] {}", msg),
                _ => tracing::warn!("[Lua] {}", msg),
            }
            Ok(())
        })?,
    )?;
    redis.raw_set(
        "sha1hex",
        lua.create_function(|_, s: LuaString| Ok(sha1_hex_bytes(&s.as_bytes())))?,
    )?;
    redis.raw_set(
        "setresp",
        lua.create_function(|_, v: i64| {
            if v != 2 && v != 3 {
                return Err(LuaError::RuntimeError(
                    "RESP version must be 2 or 3.".into(),
                ));
            }
            let mut c = run_ctx();
            c.resp = v as u8;
            set_run_ctx(c);
            Ok(())
        })?,
    )?;
    redis.raw_set(
        "set_repl",
        lua.create_function(|_, v: i64| {
            if !(0..=3).contains(&v) {
                return Err(LuaError::RuntimeError(
                    "Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE."
                        .into(),
                ));
            }
            Ok(())
        })?,
    )?;
    redis.raw_set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.raw_set("breakpoint", lua.create_function(|_, ()| Ok(false))?)?;
    redis.raw_set("debug", lua.create_function(|_, _: LuaMultiValue| Ok(()))?)?;
    let eng = engine.clone();
    redis.raw_set(
        "acl_check_cmd",
        lua.create_function(move |_, args: LuaMultiValue| {
            let (name, cmd_args) = parse_redis_call_args(&args).map_err(LuaError::RuntimeError)?;
            let Some(spec) = crate::commands::lookup(&name) else {
                return Err(LuaError::RuntimeError(
                    "Invalid command passed to redis.acl_check_cmd()".into(),
                ));
            };
            Ok(eng.script_acl_check(spec, &cmd_args).is_ok())
        })?,
    )?;
    redis.raw_set(
        "register_function",
        lua.create_function(|_, args: LuaMultiValue| register_function(args))?,
    )?;

    for (k, v) in [
        ("LOG_DEBUG", 0i64),
        ("LOG_VERBOSE", 1),
        ("LOG_NOTICE", 2),
        ("LOG_WARNING", 3),
        ("REPL_NONE", 0),
        ("REPL_AOF", 1),
        ("REPL_SLAVE", 2),
        ("REPL_REPLICA", 2),
        ("REPL_ALL", 3),
        ("REDIS_VERSION_NUM", 0x0007_0200),
    ] {
        redis.raw_set(k, v)?;
    }
    redis.raw_set("REDIS_VERSION", "7.2.0")?;
    Ok(redis)
}

/// `redis.register_function(name, callback)` or
/// `redis.register_function{function_name=…, callback=…, flags={…}, description=…}`.
fn register_function(args: LuaMultiValue) -> LuaResult<()> {
    if !run_ctx().loading {
        return Err(LuaError::RuntimeError(
            "redis.register_function can only be called on FUNCTION LOAD command".into(),
        ));
    }
    let bad = |m: &str| LuaError::RuntimeError(m.to_string());
    let mut it = args.into_iter();
    let (name, callback, flags, description) = match (it.next(), it.next()) {
        (Some(LuaValue::String(n)), Some(LuaValue::Function(f))) => (
            n.to_string_lossy().to_string(),
            f,
            ScriptFlags::default(),
            None,
        ),
        (Some(LuaValue::Table(t)), None) => {
            let mut flags = ScriptFlags::default();
            let mut name = None;
            let mut cb = None;
            let mut desc = None;
            for pair in t.pairs::<LuaValue, LuaValue>() {
                let (k, v) = pair?;
                let LuaValue::String(k) = k else {
                    return Err(bad(
                        "named argument key given to redis.register_function is not a string",
                    ));
                };
                match (k.to_string_lossy().as_ref(), v) {
                    ("function_name", LuaValue::String(s)) => {
                        name = Some(s.to_string_lossy().to_string())
                    }
                    ("callback", LuaValue::Function(f)) => cb = Some(f),
                    ("description", LuaValue::String(s)) => {
                        desc = Some(s.to_string_lossy().to_string())
                    }
                    ("flags", LuaValue::Table(fl)) => {
                        for f in fl.sequence_values::<LuaString>() {
                            let f = f?;
                            flags
                                .set(&f.to_string_lossy())
                                .map_err(|_| bad("unknown flag given"))?;
                        }
                    }
                    (other, _) => {
                        return Err(LuaError::RuntimeError(format!(
                            "unknown argument given to redis.register_function: {}",
                            other
                        )))
                    }
                }
            }
            (
                name.ok_or_else(|| {
                    bad("redis.register_function must get a function name argument")
                })?,
                cb.ok_or_else(|| bad("redis.register_function must get a callback argument"))?,
                flags,
                desc,
            )
        }
        _ => return Err(bad("wrong number of arguments to redis.register_function")),
    };
    if !valid_function_name(&name) {
        return Err(bad(
            "Function names can only contain letters, numbers, or underscores(_) and must be at \
             least one character long",
        ));
    }
    REGISTERED.with(|r| {
        let mut r = r.borrow_mut();
        if r.iter().any(|x| x.name == name) {
            return Err(bad("Function already exists in the library"));
        }
        r.push(Registered {
            name,
            callback: Some(callback),
            flags,
            description,
        });
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Parse variadic Lua arguments into (command_name, args)
// ---------------------------------------------------------------------------

fn parse_redis_call_args(args: &LuaMultiValue) -> Result<(String, Vec<Bytes>), String> {
    let mut iter = args.iter();
    let cmd_name = match iter.next() {
        None => {
            return Err("ERR Please specify at least one argument for this redis lib call".into())
        }
        Some(LuaValue::String(s)) => s.to_string_lossy().to_ascii_uppercase(),
        Some(_) => {
            return Err("ERR Lua redis lib command arguments must be strings or integers".into())
        }
    };
    let mut cmd_args = Vec::with_capacity(args.len().saturating_sub(1));
    for val in iter {
        match val {
            LuaValue::String(s) => cmd_args.push(Bytes::copy_from_slice(&s.as_bytes())),
            LuaValue::Integer(n) => cmd_args.push(Bytes::from(n.to_string())),
            LuaValue::Number(n) => cmd_args.push(Bytes::from(format_number_arg(*n))),
            _ => {
                return Err(
                    "ERR Lua redis lib command arguments must be strings or integers".into(),
                )
            }
        }
    }
    Ok((cmd_name, cmd_args))
}

/// A Lua number passed to `redis.call`, as Redis 7 prints it (the shortest
/// representation that reads back the same; whole numbers without a
/// fraction).
fn format_number_arg(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e17 {
        format!("{}", n as i64)
    } else if n.is_nan() {
        "nan".into()
    } else if n.is_infinite() {
        if n > 0.0 { "inf" } else { "-inf" }.into()
    } else {
        format!("{}", n)
    }
}

// ---------------------------------------------------------------------------
// CommandResponse → Lua value
// ---------------------------------------------------------------------------

fn response_to_lua(lua: &Lua, resp: CommandResponse, proto: u8) -> LuaResult<LuaValue> {
    match resp {
        CommandResponse::Nil | CommandResponse::NilArray => Ok(if proto == 3 {
            LuaValue::Nil
        } else {
            LuaValue::Boolean(false)
        }),
        CommandResponse::Ok => {
            let t = lua.create_table()?;
            t.raw_set("ok", "OK")?;
            Ok(LuaValue::Table(t))
        }
        CommandResponse::Integer(n) => Ok(LuaValue::Integer(n)),
        CommandResponse::BulkString(b) => Ok(LuaValue::String(lua.create_string(b.as_ref())?)),
        CommandResponse::SimpleString(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", lua.create_string(s.as_ref())?)?;
            Ok(LuaValue::Table(t))
        }
        CommandResponse::Array(items) => {
            let t = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.into_iter().enumerate() {
                t.raw_set(i + 1, response_to_lua(lua, item, proto)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        CommandResponse::Error(msg) => err_table(lua, msg),
        CommandResponse::Queued => {
            let t = lua.create_table()?;
            t.raw_set("ok", "QUEUED")?;
            Ok(LuaValue::Table(t))
        }
        other => resp3_to_lua(lua, other, proto),
    }
}

/// The RESP3-typed replies: flattened as RESP2 shows them, or Redis'
/// `{map=…}`, `{set=…}`, `{double=…}` tables under `redis.setresp(3)`.
fn resp3_to_lua(lua: &Lua, resp: CommandResponse, proto: u8) -> LuaResult<LuaValue> {
    match resp {
        CommandResponse::Double(d) => {
            if proto == 3 {
                let t = lua.create_table()?;
                t.raw_set("double", d)?;
                Ok(LuaValue::Table(t))
            } else {
                Ok(LuaValue::String(
                    lua.create_string(solikv_core::format_double(d))?,
                ))
            }
        }
        CommandResponse::Bool(b) => Ok(if proto == 3 {
            LuaValue::Boolean(b)
        } else {
            LuaValue::Integer(b as i64)
        }),
        CommandResponse::Map(pairs) => {
            if proto == 3 {
                let m = lua.create_table()?;
                for (k, v) in pairs {
                    m.raw_set(
                        response_to_lua(lua, k, proto)?,
                        response_to_lua(lua, v, proto)?,
                    )?;
                }
                let t = lua.create_table()?;
                t.raw_set("map", m)?;
                Ok(LuaValue::Table(t))
            } else {
                let t = lua.create_table_with_capacity(pairs.len() * 2, 0)?;
                let mut i = 1;
                for (k, v) in pairs {
                    t.raw_set(i, response_to_lua(lua, k, proto)?)?;
                    t.raw_set(i + 1, response_to_lua(lua, v, proto)?)?;
                    i += 2;
                }
                Ok(LuaValue::Table(t))
            }
        }
        CommandResponse::Set(items) => {
            if proto == 3 {
                let s = lua.create_table()?;
                for item in items {
                    s.raw_set(response_to_lua(lua, item, proto)?, true)?;
                }
                let t = lua.create_table()?;
                t.raw_set("set", s)?;
                Ok(LuaValue::Table(t))
            } else {
                response_to_lua(lua, CommandResponse::Array(items), proto)
            }
        }
        CommandResponse::Pairs(items) => {
            // RESP2: flat; RESP3: an array of 2-element arrays.
            if proto == 3 {
                let t = lua.create_table_with_capacity(items.len(), 0)?;
                for (i, (a, b)) in items.into_iter().enumerate() {
                    let pair = lua.create_table()?;
                    pair.raw_set(1, response_to_lua(lua, a, proto)?)?;
                    pair.raw_set(2, response_to_lua(lua, b, proto)?)?;
                    t.raw_set(i + 1, pair)?;
                }
                Ok(LuaValue::Table(t))
            } else {
                let t = lua.create_table_with_capacity(items.len() * 2, 0)?;
                let mut i = 1;
                for (a, b) in items {
                    t.raw_set(i, response_to_lua(lua, a, proto)?)?;
                    t.raw_set(i + 1, response_to_lua(lua, b, proto)?)?;
                    i += 2;
                }
                Ok(LuaValue::Table(t))
            }
        }
        CommandResponse::Verbatim(_, text) => {
            Ok(LuaValue::String(lua.create_string(text.as_ref())?))
        }
        CommandResponse::Push(items) => response_to_lua(lua, CommandResponse::Array(items), proto),
        other => response_to_lua(lua, other, proto),
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
        LuaValue::String(s) => CommandResponse::BulkString(Bytes::copy_from_slice(&s.as_bytes())),
        LuaValue::Table(t) => {
            if let Ok(LuaValue::String(err)) = t.raw_get::<LuaValue>("err") {
                let e = err.to_string_lossy();
                return CommandResponse::Error(e.strip_prefix('-').unwrap_or(&e).to_string());
            }
            if let Ok(LuaValue::String(ok)) = t.raw_get::<LuaValue>("ok") {
                return CommandResponse::SimpleString(Bytes::copy_from_slice(&ok.as_bytes()));
            }
            match t.raw_get::<LuaValue>("double") {
                Ok(LuaValue::Number(d)) => return CommandResponse::Double(d),
                Ok(LuaValue::Integer(d)) => return CommandResponse::Double(d as f64),
                _ => {}
            }
            if let Ok(LuaValue::Table(m)) = t.raw_get::<LuaValue>("map") {
                let mut pairs = Vec::new();
                for (k, v) in m.pairs::<LuaValue, LuaValue>().flatten() {
                    pairs.push((lua_to_response(k), lua_to_response(v)));
                }
                return CommandResponse::Map(pairs);
            }
            if let Ok(LuaValue::Table(s)) = t.raw_get::<LuaValue>("set") {
                let items = s
                    .pairs::<LuaValue, LuaValue>()
                    .flatten()
                    .map(|(k, _)| lua_to_response(k))
                    .collect();
                return CommandResponse::Set(items);
            }
            // An array, up to the first nil.
            let mut items = Vec::new();
            let mut i = 1;
            loop {
                match t.raw_get::<LuaValue>(i) {
                    Ok(LuaValue::Nil) | Err(_) => break,
                    Ok(v) => items.push(lua_to_response(v)),
                }
                i += 1;
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

    fn engine(shards: usize) -> Arc<CommandEngine> {
        let shards = Arc::new(crate::ShardManager::new(shards));
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let engine = Arc::new(CommandEngine::new(shards, pubsub));
        engine.init_self_ref(Arc::downgrade(&engine));
        engine
    }

    fn bulk(r: &CommandResponse) -> String {
        match r {
            CommandResponse::BulkString(b) => String::from_utf8_lossy(b).to_string(),
            other => panic!("expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_script_cache() {
        let cache = ScriptCache::new();

        let sha = cache.load("return 1");
        assert_eq!(sha.len(), 40);
        assert_eq!(cache.get(&sha).as_deref(), Some("return 1"));
        assert_eq!(cache.get("nonexistent"), None);

        let sha2 = cache.load("return 2");
        let exists = cache.exists(&[&sha, &sha2, "nonexistent"]);
        assert_eq!(exists, vec![true, true, false]);

        cache.flush();
        assert_eq!(cache.get(&sha), None);
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
        assert!(matches!(
            lua_to_response(LuaValue::Number(3.7)),
            CommandResponse::Integer(3)
        ));
    }

    #[test]
    fn shebang_flags_are_parsed() {
        let (f, body) = parse_script("#!lua flags=no-writes,allow-oom\nreturn 1").unwrap();
        assert!(f.no_writes && f.allow_oom && f.shebang);
        assert_eq!(body, "\nreturn 1");
        assert!(parse_script("#!lua flags=bogus\nreturn 1")
            .unwrap_err()
            .contains("Unexpected flag"));
        assert!(parse_script("#!python\nreturn 1")
            .unwrap_err()
            .contains("Could not find engine"));
        let (f, body) = parse_script("return 1").unwrap();
        assert!(!f.shebang);
        assert_eq!(body, "return 1");
    }

    #[test]
    fn scripts_see_lua_5_1_and_redis_libraries() {
        let e = engine(2);
        let r = execute_script(
            &e,
            "return tostring(3) .. '/' .. tostring(1.5) .. '/' .. type(unpack)",
            vec![],
            vec![],
        );
        assert_eq!(bulk(&r), "3/1.5/function");
        let r = execute_script(&e, "return 7 / 2", vec![], vec![]);
        assert!(matches!(r, CommandResponse::Integer(3)), "{r:?}");
        let r = execute_script(
            &e,
            "return cjson.encode(cmsgpack.unpack(cmsgpack.pack({a = {1, 2}})))",
            vec![],
            vec![],
        );
        assert_eq!(bulk(&r), r#"{"a":[1,2]}"#);
        let r = execute_script(
            &e,
            "return bit.band(12, 10) + struct.size('>i')",
            vec![],
            vec![],
        );
        assert!(matches!(r, CommandResponse::Integer(12)), "{r:?}");
        let r = execute_script(&e, "return redis.sha1hex('')", vec![], vec![]);
        assert_eq!(bulk(&r), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn test_execute_script_basic() {
        let e = engine(4);
        assert!(matches!(
            execute_script(&e, "return 42", vec![], vec![]),
            CommandResponse::Integer(42)
        ));
        assert_eq!(
            bulk(&execute_script(&e, "return 'hello'", vec![], vec![])),
            "hello"
        );
        assert!(matches!(
            execute_script(&e, "return nil", vec![], vec![]),
            CommandResponse::Nil
        ));
        assert!(matches!(
            execute_script(&e, "return true", vec![], vec![]),
            CommandResponse::Integer(1)
        ));
        assert!(matches!(
            execute_script(&e, "return false", vec![], vec![]),
            CommandResponse::Nil
        ));
    }

    #[test]
    fn test_execute_script_keys_argv_are_binary_safe() {
        let e = engine(4);
        assert_eq!(
            bulk(&execute_script(
                &e,
                "return KEYS[1]",
                vec![Bytes::from("mykey")],
                vec![]
            )),
            "mykey"
        );
        let r = execute_script(
            &e,
            "return #ARGV[1]",
            vec![],
            vec![Bytes::from_static(b"\xff\x00\xfe")],
        );
        assert!(matches!(r, CommandResponse::Integer(3)), "{r:?}");
    }

    #[test]
    fn test_execute_script_redis_call() {
        let e = engine(4);
        let r = execute_script(
            &e,
            "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])",
            vec![Bytes::from("luakey")],
            vec![Bytes::from("luaval")],
        );
        assert_eq!(bulk(&r), "luaval");
        // A float argument keeps its fraction (Redis 7).
        let r = execute_script(
            &e,
            "redis.call('ZADD', 'z', 1.5, 'm') return redis.call('ZSCORE', 'z', 'm')",
            vec![],
            vec![],
        );
        assert_eq!(bulk(&r), "1.5");
    }

    #[test]
    fn redis_call_errors_are_tables_and_reach_the_client_verbatim() {
        let e = engine(1);
        execute_script(&e, "redis.call('SET', 'str', 'x')", vec![], vec![]);
        let r = execute_script(&e, "return redis.call('LPUSH', 'str', 'a')", vec![], vec![]);
        assert!(
            matches!(&r, CommandResponse::Error(m) if m.starts_with("WRONGTYPE")),
            "{r:?}"
        );
        let r = execute_script(
            &e,
            "local ok, err = pcall(redis.call, 'LPUSH', 'str', 'a') return err.err",
            vec![],
            vec![],
        );
        assert!(bulk(&r).starts_with("WRONGTYPE"));
        let r = execute_script(
            &e,
            "local r = redis.pcall('EVAL', 'return 1', '0') return r.err",
            vec![],
            vec![],
        );
        assert!(bulk(&r).contains("not allowed from script"));
        let r = execute_script(
            &e,
            "return redis.error_reply('ERR custom error')",
            vec![],
            vec![],
        );
        assert!(
            matches!(&r, CommandResponse::Error(m) if m == "ERR custom error"),
            "{r:?}"
        );
        let r = execute_script(&e, "return {err='My Error'}", vec![], vec![]);
        assert!(
            matches!(&r, CommandResponse::Error(m) if m == "My Error"),
            "{r:?}"
        );
        let r = execute_script(&e, "return redis.status_reply('PONG')", vec![], vec![]);
        assert!(matches!(&r, CommandResponse::SimpleString(s) if s.as_ref() == b"PONG"));
        let r = execute_script(&e, "invalid lua !!!", vec![], vec![]);
        assert!(matches!(&r, CommandResponse::Error(m) if m.starts_with("ERR")));
        let r = execute_script(&e, "return {1, 2, 3, nil, 5}", vec![], vec![]);
        assert!(matches!(&r, CommandResponse::Array(v) if v.len() == 3));
    }

    #[test]
    fn globals_and_libraries_are_read_only() {
        let e = engine(1);
        for (script, want) in [
            ("x = 5 return 1", "readonly"),
            ("_G.leaked = 1 return 1", "readonly"),
            ("string.rep = nil return 1", "readonly"),
            ("redis.call = 1 return 1", "readonly"),
            ("return undefined_thing", "nonexistent global"),
        ] {
            let r = execute_script(&e, script, vec![], vec![]);
            assert!(
                matches!(&r, CommandResponse::Error(m) if m.contains(want)),
                "{script}: {r:?}"
            );
        }
        // Going around the guards does not reach the next script.
        for evil in [
            "rawset(_G, 'leaked', 'x') return 1",
            "rawset(string, 'upper', function() return 'pwned' end) return 1",
            "rawset(redis, 'call', function() return 'pwned' end) return 1",
        ] {
            let _ = execute_script(&e, evil, vec![], vec![]);
            let r = execute_script(
                &e,
                "redis.call('SET', 'k', 'v') return string.upper(redis.call('GET', 'k'))",
                vec![],
                vec![],
            );
            assert_eq!(bulk(&r), "V", "{evil}");
            let r = execute_script(
                &e,
                "return rawget(_G, 'leaked') == nil and 1 or 0",
                vec![],
                vec![],
            );
            assert!(matches!(r, CommandResponse::Integer(1)), "{evil}: {r:?}");
        }
        let r = execute_script(&e, "return getmetatable('')", vec![], vec![]);
        assert!(matches!(r, CommandResponse::Nil), "{r:?}");
        for gone in [
            "loadstring",
            "setfenv",
            "getfenv",
            "dofile",
            "debug",
            "io",
            "require",
            "print",
        ] {
            let r = execute_script(&e, &format!("return {gone}"), vec![], vec![]);
            assert!(matches!(&r, CommandResponse::Error(_)), "{gone}: {r:?}");
        }
    }

    #[test]
    fn read_only_scripts_refuse_writes() {
        let e = engine(1);
        let r = execute_script_with(
            &e,
            "return redis.call('SET', 'k', 'v')",
            vec![],
            vec![],
            true,
        );
        assert!(
            matches!(&r, CommandResponse::Error(m) if m.contains("read-only scripts")),
            "{r:?}"
        );
        let r = execute_script_with(&e, "return redis.call('GET', 'k')", vec![], vec![], true);
        assert!(matches!(r, CommandResponse::Nil), "{r:?}");
    }

    #[test]
    fn compiled_scripts_are_reused_with_fresh_arguments() {
        let e = engine(1);
        let script = "return KEYS[1] .. ':' .. ARGV[1]";
        for i in 0..5 {
            let r = execute_script(
                &e,
                script,
                vec![Bytes::from(format!("k{i}"))],
                vec![Bytes::from(format!("a{i}"))],
            );
            assert_eq!(bulk(&r), format!("k{i}:a{i}"));
        }
    }

    #[test]
    fn script_kill_is_not_swallowed_by_pcall() {
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(20));
            eprintln!("WATCHDOG: script was never killed");
            std::process::exit(3);
        });
        for script in [
            "while true do end",
            "while true do pcall(function() while true do end end) end",
        ] {
            let e = engine(1);
            let e2 = e.clone();
            let h = std::thread::spawn(move || {
                e2.execute("EVAL", &[Bytes::from(script), Bytes::from("0")])
            });
            std::thread::sleep(std::time::Duration::from_millis(300));
            let k = e.script_state().kill(false);
            eprintln!("kill -> {k:?} running={}", e.script_state().is_running());
            let r = h.join().unwrap();
            assert!(
                matches!(&r, CommandResponse::Error(m) if m.contains("killed")),
                "{script}: {r:?}"
            );
        }
    }

    #[test]
    fn functions_load_and_run() {
        let e = engine(2);
        let code = "#!lua name=mylib\n\
                    redis.register_function('myset', function(keys, args) return redis.call('SET', keys[1], args[1]) end)\n\
                    redis.register_function{function_name='myget', callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
        assert_eq!(load_library(&e, code, false).unwrap(), "mylib");
        assert!(load_library(&e, code, false)
            .unwrap_err()
            .contains("already exists"));
        assert_eq!(load_library(&e, code, true).unwrap(), "mylib");
        let r = execute_function(
            &e,
            "myset",
            vec![Bytes::from("k")],
            vec![Bytes::from("v")],
            false,
        );
        assert!(matches!(r, CommandResponse::SimpleString(_)), "{r:?}");
        let r = execute_function(&e, "myget", vec![Bytes::from("k")], vec![], true);
        assert_eq!(bulk(&r), "v");
        assert!(e.functions().function("myget").unwrap().flags.no_writes);
        assert!(load_library(&e, "return 1", false)
            .unwrap_err()
            .contains("Missing library metadata"));
        assert!(load_library(&e, "#!lua name=empty\nlocal x = 1", false)
            .unwrap_err()
            .contains("No functions registered"));
        let dump = e.functions().dump();
        assert_eq!(parse_function_dump(&dump).unwrap().len(), 1);
        assert!(e.functions().delete("mylib"));
        let r = execute_function(&e, "myget", vec![Bytes::from("k")], vec![], true);
        assert!(
            matches!(&r, CommandResponse::Error(m) if m.contains("Function not found")),
            "{r:?}"
        );
    }
}
