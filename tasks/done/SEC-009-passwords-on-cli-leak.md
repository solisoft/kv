# SEC-009: `--requirepass` and `--cluster-password` exposed via /proc/<pid>/cmdline and shell history

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-server/src/main.rs:65-67`, `main.rs:97-99`

**Issue:** Both `--requirepass <PASSWORD>` and `--cluster-password <PASSWORD>` are clap arguments with the value baked into argv. On Linux, every local user can read the live argv of every process from `/proc/<pid>/cmdline`. Daemon mode also re-launches with `std::env::args().skip(1)` (`main.rs:140-149`), which faithfully forwards the password to the child process's argv. The same value also ends up in shell history, in `ps aux`, in container telemetry, and in `journalctl -u solikv` if any wrapper logs the launch command line.

**Fix:** Accept passwords via:
- An env var (`SOLIKV_REQUIREPASS`, `SOLIKV_CLUSTER_PASSWORD`).
- A file path (`--requirepass-file /path/to/secret`) read once at startup with `O_NOFOLLOW`, mode 0600 enforced.
- Piped on stdin when `--requirepass -` is used.

Keep the `--requirepass` literal form for compat but emit a `tracing::warn` ("password supplied on command line is visible to other local users") and zero out the argv buffer after parsing where feasible.
