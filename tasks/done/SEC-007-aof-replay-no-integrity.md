# SEC-007: AOF replay executes arbitrary commands from disk with no integrity check

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-server/src/main.rs:378-401`, `crates/solikv-persist/src/aof.rs:159-223`

**Issue:** On startup, if `--appendonly` is enabled, `AofPersistence::replay` parses the on-disk `appendonly.aof` and feeds every entry verbatim to `replay_engine.execute(&name, &cmd_args[1..])`. The dispatcher then runs `EVAL`, `CONFIG SET`, `FLUSHALL`, `BF.RESERVE`, etc. exactly as if a client had issued them. There is no MAC/HMAC/signature on the AOF, no length sanity bound on the file, and no "trusted bit" tracking. An attacker with write access to `args.dir/appendonly.aof` can prepend a command stream that runs at the next restart with full server privileges (and inside the Lua sandbox runs whatever the script is allowed to do).

**Fix:** At minimum, fail-closed when the AOF contains an explicitly disallowed command for replay (e.g. `EVAL` / `EVALSHA` should be rewritten as `SCRIPT LOAD` + ops, never as raw EVAL — `main.rs` already special-cases `EVALSHA → EVAL` rewriting on write but not on read). Better: chain-hash every appended record and refuse to replay past the first mismatch; even better, sign/MAC the file with a key derived from `requirepass` or a separate key-file. Document that the data directory must be on a filesystem only the solikv UID can write to.
