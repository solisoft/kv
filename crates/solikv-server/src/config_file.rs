//! `redis.conf`-style configuration files.
//!
//! `solikv /etc/solikv.conf` or `solikv --config /etc/solikv.conf` reads
//! directives — `name value…`, one per line, `#` comments, `"quoted"` and
//! `'quoted'` values, `include other.conf` — and turns each into the
//! command-line flag of the same name, so the file and the flags are one
//! set of settings. Flags given on the command line are placed after the
//! file's, and win.
//!
//! Redis directives that mean nothing here (tuning of Redis' own encodings,
//! replication, `supervised`, …) are accepted and ignored with a warning, so
//! a stock `redis.conf` loads; an unknown directive is an error, as in
//! Redis.

use std::path::{Path, PathBuf};

/// Redis directives with no counterpart here.
const IGNORED: &[&str] = &[
    "tcp-backlog",
    "tcp-keepalive",
    "supervised",
    "pidfile",
    "always-show-logo",
    "set-proc-title",
    "proc-title-template",
    "locale-collate",
    "stop-writes-on-bgsave-error",
    "rdbcompression",
    "rdbchecksum",
    "sanitize-dump-payload",
    "rdb-del-sync-files",
    "replica-serve-stale-data",
    "replica-read-only",
    "repl-diskless-sync",
    "repl-diskless-sync-delay",
    "repl-diskless-sync-max-replicas",
    "repl-diskless-load",
    "repl-disable-tcp-nodelay",
    "replica-priority",
    "lazyfree-lazy-eviction",
    "lazyfree-lazy-expire",
    "lazyfree-lazy-server-del",
    "replica-lazy-flush",
    "lazyfree-lazy-user-del",
    "lazyfree-lazy-user-flush",
    "oom-score-adj",
    "oom-score-adj-values",
    "disable-thp",
    "appendfilename",
    "no-appendfsync-on-rewrite",
    "aof-load-truncated",
    "aof-use-rdb-preamble",
    "aof-timestamp-enabled",
    "slowlog-log-slower-than-ms",
    "hash-max-listpack-entries",
    "hash-max-listpack-value",
    "hash-max-ziplist-entries",
    "hash-max-ziplist-value",
    "list-max-listpack-size",
    "list-max-ziplist-size",
    "list-compress-depth",
    "set-max-intset-entries",
    "set-max-listpack-entries",
    "set-max-listpack-value",
    "zset-max-listpack-entries",
    "zset-max-listpack-value",
    "zset-max-ziplist-entries",
    "zset-max-ziplist-value",
    "hll-sparse-max-bytes",
    "stream-node-max-bytes",
    "stream-node-max-entries",
    "activerehashing",
    "client-output-buffer-limit",
    "dynamic-hz",
    "aof-rewrite-incremental-fsync",
    "rdb-save-incremental-fsync",
    "jemalloc-bg-thread",
    "io-threads",
    "io-threads-do-reads",
    "lua-replicate-commands",
    "activedefrag",
    "active-defrag-ignore-bytes",
    "active-defrag-threshold-lower",
    "active-defrag-threshold-upper",
    "active-defrag-cycle-min",
    "active-defrag-cycle-max",
    "active-defrag-max-scan-fields",
    "lfu-log-factor",
    "lfu-decay-time",
    "maxmemory-eviction-tenacity",
    "syslog-enabled",
    "syslog-ident",
    "syslog-facility",
    "crash-log-enabled",
    "crash-memcheck-enabled",
    "shutdown-timeout",
    "shutdown-on-sigint",
    "shutdown-on-sigterm",
    "latency-tracking",
    "latency-tracking-info-percentiles",
    "acllog-max-len",
    "enable-protected-configs",
    "enable-module-command",
    "cluster-config-file",
    "cluster-node-timeout",
    "cluster-port",
    "cluster-replica-validity-factor",
    "cluster-migration-barrier",
    "cluster-allow-replica-migration",
    "cluster-require-full-coverage",
    "cluster-replica-no-failover",
    "cluster-allow-reads-when-down",
    "cluster-allow-pubsubshard-when-down",
    "cluster-link-sendbuf-limit",
    "cluster-announce-hostname",
    "cluster-preferred-endpoint-type",
    "repl-backlog-size",
    "repl-backlog-ttl",
    "repl-timeout",
    "repl-ping-replica-period",
    "min-replicas-to-write",
    "min-replicas-max-lag",
    "replica-announced",
    "unixsocket",
    "unixsocketperm",
    "rename-command",
];

/// Directives whose flag has another name here.
fn flag_name(directive: &str) -> &str {
    match directive {
        "tls-cert-file" => "tls-cert",
        "tls-key-file" => "tls-key",
        "tls-ca-cert-file" => "tls-client-ca",
        "loglevel" => "log-level",
        "lua-time-limit" => "busy-reply-threshold",
        other => other,
    }
}

/// Flags that take no value (present when the directive says yes).
const SWITCHES: &[&str] = &["daemon", "daemonize", "cluster-enabled", "solo"];

/// Split one line like Redis' `sdssplitargs`: whitespace-separated, with
/// double quotes (C escapes) and single quotes.
pub fn split_args(line: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&c) = chars.peek() else {
            return Ok(out);
        };
        let mut cur = String::new();
        match c {
            '"' => {
                chars.next();
                loop {
                    match chars.next() {
                        None => return Err("unbalanced quotes".into()),
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some('n') => cur.push('\n'),
                            Some('r') => cur.push('\r'),
                            Some('t') => cur.push('\t'),
                            Some('b') => cur.push('\u{8}'),
                            Some('a') => cur.push('\u{7}'),
                            Some('x') => {
                                let h: String =
                                    [chars.next(), chars.next()].iter().flatten().collect();
                                match u8::from_str_radix(&h, 16) {
                                    Ok(b) => cur.push(b as char),
                                    Err(_) => {
                                        cur.push('x');
                                        cur.push_str(&h);
                                    }
                                }
                            }
                            Some(other) => cur.push(other),
                            None => return Err("unbalanced quotes".into()),
                        },
                        Some(other) => cur.push(other),
                    }
                }
                if chars.peek().is_some_and(|c| !c.is_whitespace()) {
                    return Err("closing quote must be followed by a space".into());
                }
            }
            '\'' => {
                chars.next();
                loop {
                    match chars.next() {
                        None => return Err("unbalanced quotes".into()),
                        Some('\'') => break,
                        Some('\\') if chars.peek() == Some(&'\'') => {
                            chars.next();
                            cur.push('\'');
                        }
                        Some(other) => cur.push(other),
                    }
                }
            }
            _ => {
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    cur.push(c);
                    chars.next();
                }
            }
        }
        out.push(cur);
    }
}

fn yes(v: &str) -> Result<bool, String> {
    match v.to_ascii_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(format!("argument must be 'yes' or 'no', got '{}'", v)),
    }
}

/// The flags a configuration file stands for. `known` answers whether a
/// flag (without `--`) exists.
pub fn load(path: &Path, known: &dyn Fn(&str) -> bool) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = Vec::new();
    load_into(path, known, &mut out, &mut seen, 0)?;
    Ok(out)
}

fn load_into(
    path: &Path,
    known: &dyn Fn(&str) -> bool,
    out: &mut Vec<String>,
    seen: &mut Vec<PathBuf>,
    depth: usize,
) -> Result<(), String> {
    if depth > 16 {
        return Err(format!("{}: includes nested too deeply", path.display()));
    }
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    if seen.contains(&canon) {
        return Ok(());
    }
    seen.push(canon);
    let text = std::fs::read_to_string(path).map_err(|e| {
        format!(
            "Fatal error, can't open config file '{}': {}",
            path.display(),
            e
        )
    })?;
    for (n, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let bad = |why: &str| {
            format!(
                "*** FATAL CONFIG FILE ERROR ***\nReading the configuration file, at line {} of {}\n>>> '{}'\n{}",
                n + 1,
                path.display(),
                line,
                why
            )
        };
        let args = split_args(line).map_err(|e| bad(&e))?;
        let Some((directive, values)) = args.split_first() else {
            continue;
        };
        let directive = directive.to_ascii_lowercase();
        if directive == "include" {
            for v in values {
                let inc = Path::new(v);
                let inc = if inc.is_relative() {
                    path.parent().unwrap_or(Path::new(".")).join(inc)
                } else {
                    inc.to_path_buf()
                };
                load_into(&inc, known, out, seen, depth + 1)?;
            }
            continue;
        }
        if IGNORED.contains(&directive.as_str()) {
            eprintln!(
                "WARNING: config directive '{}' ({}:{}) has no effect in SoliKV",
                directive,
                path.display(),
                n + 1
            );
            continue;
        }
        match directive.as_str() {
            "save" => {
                // `save <seconds> <changes> …` → the shortest interval;
                // `save ""` → no snapshots.
                let secs = values
                    .chunks(2)
                    .filter_map(|p| p.first()?.parse::<u64>().ok())
                    .min()
                    .unwrap_or(0);
                out.push(format!("--save-interval={}", secs));
            }
            "appendonly" => {
                let v = values
                    .first()
                    .ok_or_else(|| bad("wrong number of arguments"))?;
                out.push(format!("--appendonly={}", yes(v).map_err(|e| bad(&e))?));
            }
            "loglevel" => {
                let v = values
                    .first()
                    .ok_or_else(|| bad("wrong number of arguments"))?;
                let level = match v.to_ascii_lowercase().as_str() {
                    "debug" => "debug",
                    "verbose" => "debug",
                    "notice" => "info",
                    "warning" => "warn",
                    "nothing" => "error",
                    other => other,
                }
                .to_string();
                out.push(format!("--log-level={}", level));
            }
            "bind" => {
                // Redis binds several addresses; the first is used here.
                let v = values
                    .iter()
                    .map(|v| v.trim_start_matches('-'))
                    .next()
                    .ok_or_else(|| bad("wrong number of arguments"))?;
                out.push(format!("--bind={}", v));
            }
            d if SWITCHES.contains(&d) => {
                let v = values.first().map(String::as_str).unwrap_or("yes");
                if yes(v).map_err(|e| bad(&e))? {
                    out.push(format!("--{}", if d == "daemonize" { "daemon" } else { d }));
                }
            }
            d => {
                let flag = flag_name(d);
                if !known(flag) {
                    return Err(bad("Bad directive or wrong number of arguments"));
                }
                match values {
                    [] => return Err(bad("wrong number of arguments")),
                    [v] => out.push(format!("--{}={}", flag, v)),
                    many => out.push(format!("--{}={}", flag, many.join(" "))),
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_like_sdssplitargs() {
        assert_eq!(split_args("a b  c").unwrap(), vec!["a", "b", "c"]);
        assert_eq!(
            split_args(r#"requirepass "p w\"d" x"#).unwrap(),
            vec!["requirepass", "p w\"d", "x"]
        );
        assert_eq!(split_args("save ''").unwrap(), vec!["save", ""]);
        assert!(split_args(r#"a "unterminated"#).is_err());
    }

    #[test]
    fn a_file_becomes_flags() {
        let dir = std::env::temp_dir().join(format!("solikv-conf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let inc = dir.join("inc.conf");
        std::fs::write(&inc, "maxmemory-policy allkeys-lru\n").unwrap();
        let main = dir.join("main.conf");
        std::fs::write(
            &main,
            "# comment\nport 7000\nsave 900 1 300 10\nappendonly no\ntcp-keepalive 300\n\
             include inc.conf\nloglevel notice\nbind 127.0.0.1 -::1\n",
        )
        .unwrap();
        let known = |f: &str| matches!(f, "port" | "maxmemory-policy");
        let flags = load(&main, &known).unwrap();
        assert_eq!(
            flags,
            vec![
                "--port=7000",
                "--save-interval=300",
                "--appendonly=false",
                "--maxmemory-policy=allkeys-lru",
                "--log-level=info",
                "--bind=127.0.0.1",
            ]
        );
        std::fs::write(&main, "nonsense-directive 1\n").unwrap();
        assert!(load(&main, &known).unwrap_err().contains("Bad directive"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
