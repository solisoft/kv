# SEC-014: Pooled Lua VMs reused across scripts without resetting user-set globals

- **Severity:** Low
- **Status:** Todo
- **Location:** `crates/solikv-engine/src/lua.rs:103-138`, `lua.rs:147-186`

**Issue:** `take_lua` returns thread-local `PooledLua` instances that survive across `EVAL`/`EVALSHA` calls. Sandboxing (`sandbox_lua`) and the `redis.*` module install run **once** on cold path; subsequent reuses skip both. `setup_keys_argv` overwrites `KEYS`/`ARGV` each call, but any other global a previous script wrote (`_G.foo = 'secret'`, modifications to the `string` / `math` / `table` tables) persists into the next script that lands on the same VM and the same OS thread. With a single shared `requirepass`, the sandbox typically only guards against logic mistakes inside scripts — but if a future change introduces per-tenant or per-user scopes, this becomes cross-tenant state leakage. Today it is a hardening / determinism issue: scripts are not idempotent.

It also means a script can poison the VM (e.g. monkey-patch `string.upper`) and affect all subsequent scripts running on that thread until the VM is dropped from the pool (which only happens when the pool is full).

**Fix:** Either (a) snapshot the post-sandbox global table on first creation and restore from snapshot before each `eval`, or (b) drop the VM after every script (lose pool reuse, simpler), or (c) run scripts inside a fresh Lua *thread* (`coroutine`) with an explicit `_ENV` derived from a frozen template. Option (a) preserves the perf win.
