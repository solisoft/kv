# SEC-003 — Lua VM is pooled and reused without resetting global state, allowing sandbox poisoning across scripts

**Severity:** High

## Location

- `crates/solikv-engine/src/lua.rs:103-138` (`LUA_POOL`, `take_lua`, `return_lua`)
- `crates/solikv-engine/src/lua.rs:147-186` (`execute_script`)
- `crates/solikv-engine/src/lua.rs:192-209` (`sandbox_lua` runs once on cold path only)

## Issue

`take_lua` reuses a previously-sandboxed `Lua` instance from a thread-local
`LUA_POOL`. The sandbox (`sandbox_lua`) is applied once on creation, and only
`KEYS` / `ARGV` are reset between invocations. Globals other than `KEYS` /
`ARGV` — including the standard-library tables `string`, `table`, `math` —
are *not* restored between scripts.

A malicious script can therefore plant traps that affect future scripts running
on the same worker thread:

```lua
-- attacker:  EVAL "string.byte = function() return 999 end ..."
-- victim:    EVAL "return string.byte('x')"   →  999, not 120
```

Or worse, a script can monkey-patch `redis.call` itself:

```lua
local orig = redis.call
redis.call = function(...) ... return "" end   -- silent data corruption
```

Because the VM is pooled per-thread and reused, this contaminates other tenants
sharing the server. `redis` itself is set in `setup_redis_module` only on cold
path (line 124), so a poisoned `redis.call` survives every subsequent
`execute_script` on that thread until the VM is dropped (pool is full).

## Fix

Either (preferred):

1. Re-snapshot the global table after `sandbox_lua` + `setup_redis_module`
   completes (e.g., serialize a registry-stashed clean copy), and restore it on
   each `take_lua`. mlua exposes `Lua::globals()` and `Table::clear()` /
   pairs traversal to rebuild it.

Or, simpler:

2. Drop the pool entirely — create a fresh `Lua` per script. The cost is one
   sandbox setup per EVAL, which dominates only for trivially small scripts.
   Benchmark first.

Or:

3. Wrap user scripts in a fresh Lua function environment (`setfenv` /
   `_ENV` swap in 5.2+) so each script gets its own `_ENV` with read-only
   parent. Verify mlua exposes the necessary primitives.

Option 1 or 3 preserves the perf benefit. Option 2 is simplest and safest.

## Verification

- New test: run two scripts back-to-back on the same engine; the first mutates
  `string.byte`, the second calls `string.byte('x')` and must observe the
  original value.
- Existing Lua tests still pass.
