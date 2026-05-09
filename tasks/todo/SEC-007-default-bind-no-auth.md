# SEC-007 — Default bind is `0.0.0.0` with no password required, exposing an unauthenticated DB to the network

**Severity:** High

## Location

- `crates/solikv-server/src/main.rs:38-39` (`#[arg(long, default_value = "0.0.0.0")] bind`)
- `crates/solikv-server/src/main.rs:65-67` (`requirepass: Option<String>` — no default)
- `crates/solikv-server/src/resp_server.rs:80` (`requires_auth = password.is_some()`)

## Issue

A user who runs `solikv` with no flags gets:

- RESP server listening on `0.0.0.0:6379`
- REST API on `0.0.0.0:5020`
- No authentication required (`password = None` ⇒ `requires_auth = false`)

This exposes the entire database to anyone on the same network. The same
default tripped Redis for years and led to a documented mass-exploitation
campaign ("Redis WCRY"). The README's quickstart and Docker example both run
this way.

The cluster-bus port at least binds to `127.0.0.1` only (`gossip.rs:336`), but
the public RESP/REST surface does not.

## Fix

Pick one of these (in order of preference):

1. **Refuse to start unauthenticated when bind is non-loopback.** If
   `args.bind != "127.0.0.1"` and `args.requirepass.is_none()`, exit with
   a clear error pointing at `--requirepass` and `--bind 127.0.0.1`. Provide
   `--protected-mode no` opt-out for users who really mean it.
2. Change the default `bind` to `127.0.0.1`. Document `--bind 0.0.0.0` for
   network exposure.

Approach 1 is closer to upstream Redis "protected mode" and is what most
Redis-compatible projects do. Approach 2 is simpler but slightly less
forgiving.

Also update the README quickstart and Docker examples to set `--requirepass`
and/or `--bind 127.0.0.1`, and note the new behavior in
`www/app/views/docs/getting_started.html.slv`.

## Verification

- Unit: `Args::parse_from(["solikv", "--bind", "0.0.0.0"])` plus the
  protected-mode check returns an error.
- Manual: `cargo run` with no flags refuses to bind public; `cargo run --
  --bind 0.0.0.0 --requirepass foo` works.
