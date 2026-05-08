# SEC-005 — Passwords accepted only via CLI flags, exposing them in `ps` / `/proc/<pid>/cmdline` / shell history

**Severity:** High

## Location

- `crates/solikv-server/src/main.rs:65-67` (`--requirepass`)
- `crates/solikv-server/src/main.rs:97-99` (`--cluster-password`)

## Issue

The auth password and cluster password are accepted only as `clap` arguments
with `value_name = "PASSWORD"`. On Linux:

- `ps auxe`, `/proc/<pid>/cmdline`, and any monitoring tool that reads cmdlines
  expose the password to other local users.
- The launching shell's history (`.bash_history`, `.zsh_history`) records it.
- The daemon code (`main.rs:140-149`) re-`spawn`s itself with
  `std::env::args().skip(1)`, so the child process *also* has the password in
  its cmdline.

Production deployments commonly leak Redis passwords this way; SoliKV inherits
the same risk and has no alternative input mechanism.

## Fix

Add support for password sources, in priority order:

1. `--requirepass-file <path>` / `--cluster-password-file <path>`: read a file
   (newline-trimmed). Recommended for production.
2. `SOLIKV_PASSWORD` / `SOLIKV_CLUSTER_PASSWORD` environment variables (clap's
   `env = "..."` attribute).
3. Keep `--requirepass <PASS>` for dev convenience but **emit a warning to
   stderr** when used so operators see the leak risk.

Refuse to start if both flag and file/env are set, to avoid silent precedence
confusion.

When the file path is used, verify mode 0600 (warn loudly if world-readable).

## Verification

- Unit: parse args from a file successfully; warn when flag is used.
- Manual: `ps -ef | grep solikv` after start with `--requirepass-file` shows
  no password in cmdline.
