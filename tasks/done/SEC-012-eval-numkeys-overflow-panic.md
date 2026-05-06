# SEC-012: EVAL/EVALSHA `numkeys` parse + slice can integer-overflow into a connection panic

- **Severity:** Low
- **Status:** Todo
- **Location:** `crates/solikv-engine/src/dispatch.rs:3216-3233`, `dispatch.rs:3260-3277`

**Issue:** `numkeys: usize` is parsed straight from a client-controlled string. The arity check is `if args.len() < 2 + numkeys`, then the code slices `args[2..2 + numkeys]` and `args[2 + numkeys..]`. In release builds `2 + numkeys` is wrapping addition; with `numkeys = usize::MAX`, the expression wraps to `1`, the bound check passes, and the subsequent slice panics. Tokio catches the task panic and drops the connection — not a process-level crash, but a free way to terminate any authenticated session and to spam stack traces into logs.

**Fix:** Use `numkeys.checked_add(2)` and return `ERR Number of keys can't be greater than number of args` on overflow. Apply the same pattern to any other dispatcher arm that parses `usize` from `args` (search for `2 + ` and `1 + ` adjacent to slice indexing). A repository-wide `cargo +nightly clippy -- -W clippy::arithmetic_side_effects` run would surface the rest.
