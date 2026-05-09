# SEC-004 — Script cache (`SCRIPT LOAD` / `EVAL`) has no size cap, enabling memory-exhaustion DoS

**Severity:** High

## Location

- `crates/solikv-engine/src/lua.rs:20-58` (`ScriptCache`)
- `crates/solikv-engine/src/dispatch.rs:3236` (`EVAL` calls `script_cache.load(script)` on every invocation)
- `crates/solikv-engine/src/dispatch.rs:3286-3303` (`SCRIPT LOAD`)

## Issue

`ScriptCache` is a `DashMap<String, String>` with no eviction, no maximum
entry count, and no maximum source size. Any authenticated client can call:

```
SCRIPT LOAD "<random 1 MiB script body>"
```

repeatedly, and each insert is permanent. With ~1 KiB scripts the server holds
~10 MB after 10 000 calls; with 1 MiB scripts, 10 GB after 10 000 calls. There
is no command to flush only stale entries, and even `SCRIPT FLUSH` requires the
attacker's cooperation.

`EVAL` *also* re-inserts the script source on every execution (line 3236), so
the cache grows on the regular EVAL hot path too — not just on `SCRIPT LOAD`.

## Fix

1. Cap script source size at a sane limit (suggested: 64 KiB, configurable).
   Reject larger scripts with `ERR script too long`.
2. Cap total script count (suggested: 8 192 entries) and switch the storage
   from `DashMap` to a small LRU (e.g. `lru::LruCache` behind a `Mutex`,
   contention is fine — script load is rare).
3. In `EVAL`, only insert into the cache if the SHA is not already present
   (cheap `contains_key` check), to avoid the rewrite-on-every-execution
   churn.

## Verification

- Unit: load N scripts where N > cache cap, assert oldest is evicted.
- Unit: `EVAL` of an oversize script returns `ERR script too long`.
- Existing scripting tests still pass.
