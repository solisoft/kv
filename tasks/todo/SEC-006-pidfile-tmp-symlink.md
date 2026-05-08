# SEC-006 — Daemon mode uses world-writable `/tmp/solikv.pid` + `/tmp/solikv.log`, with weak process verification

**Severity:** High

## Location

- `crates/solikv-server/src/main.rs:106` (`pidfile = PathBuf::from("/tmp/solikv.pid")`)
- `crates/solikv-server/src/main.rs:108-130` (kill-old logic)
- `crates/solikv-server/src/main.rs:132-138` (`/tmp/solikv.log` opened with `create + append`)

## Issue

Three problems compound:

1. **Symlink attack on the log file.** `OpenOptions::new().create(true).append(true)
   .open("/tmp/solikv.log")` follows existing symlinks. On a multi-user host an
   attacker who controls `/tmp/solikv.log` (it is in a sticky-bit directory but
   they can win the race or pre-place a symlink before solikv first runs) can
   point it at any path the operator can write to (e.g. `/etc/something`,
   `~/.bashrc`). solikv then appends its stdout/stderr there.

2. **PID file race + weak verification.** Lines 113-122 read `/proc/<pid>/cmdline`
   and only check `.contains("solikv")`. Any cmdline containing the substring
   "solikv" passes — including a user's `vim /tmp/solikv-notes`, `grep solikv
   …`, etc. Combined with the predictable `/tmp/solikv.pid` path (which a local
   attacker can pre-write), this lets the attacker get solikv to send `kill` to
   an arbitrary PID belonging to the operator (so long as that process's
   cmdline mentions "solikv").

3. **Use of `kill(1)` subprocess** for what is a built-in syscall — extra fork,
   extra attack surface, and silently swallows errors with `let _ = ... .output()`.

## Fix

1. Move pidfile and log to `args.dir` (the operator-supplied data directory),
   defaulting to `./data`. Refuse to operate on `/tmp` paths.
2. Open the log with `OpenOptions::new().custom_flags(libc::O_NOFOLLOW)` (Unix)
   to refuse symlinks; or open with `O_CREAT | O_EXCL` first and fall back to
   append-after-stat-check.
3. Strengthen the PID check: parse `/proc/<pid>/exe` (a symlink to the binary)
   and compare to `std::env::current_exe()`. `cmdline.contains("solikv")` is
   not a security check.
4. Replace the `Command::new("kill")` subprocess with `nix::sys::signal::kill`
   or `libc::kill` directly.
5. After unlinking the stale pidfile, write the new one with `O_EXCL` so two
   concurrent daemon starts cannot both succeed.

## Verification

- Unit (where feasible): given a PID whose `/proc/<pid>/exe` does not equal our
  current exe, the kill path is a no-op.
- Manual: `ln -s /tmp/victim /tmp/solikv.log`, run `solikv -d`, confirm solikv
  refuses (or creates a new file separate from the symlink target).
