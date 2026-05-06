# SEC-003: Predictable /tmp PID file with substring "solikv" cmdline check

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-server/src/main.rs:106-130`

**Issue:** Daemon mode reads `/tmp/solikv.pid` and runs `kill <pid>` if `/proc/<pid>/cmdline` *contains* the substring `"solikv"`. `/tmp` is shared across users; a local attacker can pre-create `/tmp/solikv.pid` (or symlink it) before the operator first runs `solikv -d`, pinning the PID to one of their own processes whose cmdline includes the literal `"solikv"` (e.g. `vim solikv.conf`, another user's solikv instance, etc.) and getting it SIGTERM'd. The substring-only match also happily matches on filenames passed as args, so the heuristic is essentially "any process that mentions solikv anywhere on its command line". The kill is invoked through PATH (`Command::new("kill")`) so a hostile early `PATH` entry executes its own `kill` binary as the daemon-launcher's UID.

**Fix:** Move the PID file to `args.dir` (the data directory the operator already controls) or `/var/run/solikv/solikv.pid`. Open it with `O_NOFOLLOW`. Verify the target process by reading `/proc/<pid>/exe` (resolved real path) and comparing against `current_exe()`, not by substring. Call `libc::kill` directly instead of shelling out, or use the absolute path `/bin/kill`.
