use mimalloc::MiMalloc;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

/// mimalloc, counted: `maxmemory` and `INFO memory` read the running total
/// (see `solikv_core::memory`).
#[global_allocator]
static GLOBAL: solikv_core::memory::Counting<MiMalloc> = solikv_core::memory::Counting(MiMalloc);

use clap::Parser;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use nix::sys::signal::kill;
use nix::unistd::Pid;

use solikv_persist::FsyncPolicy;

use solikv_cluster::ClusterManager;
use solikv_server::cluster_dump;
use solikv_server::resp_server;
use solikv_server::rest_server;

#[derive(Parser, Debug)]
#[command(
    name = "solikv",
    version,
    about = "SoliKV - High-Performance In-Memory Database",
    args_override_self = true
)]
struct Args {
    /// Configuration file (redis.conf syntax). Also accepted as the first
    /// argument (`solikv /etc/solikv.conf`). Flags on the command line
    /// override the file.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// A configuration file given as the first argument.
    #[arg(value_name = "CONFIG_FILE", hide = true)]
    config_positional: Option<PathBuf>,

    /// ACL users file (`ACL LOAD` / `ACL SAVE`); loaded at startup if present
    #[arg(long, value_name = "PATH")]
    aclfile: Option<PathBuf>,

    /// DEBUG command: no, local (loopback clients only) or yes
    #[arg(long, default_value = "no")]
    enable_debug_command: String,

    /// Most clients connected at once (Redis `maxclients`)
    #[arg(long, default_value = "10000")]
    maxclients: usize,

    /// Commands slower than this many microseconds go to the slow log
    /// (negative disables, 0 logs every command)
    #[arg(long, default_value = "10000", allow_hyphen_values = true)]
    slowlog_log_slower_than: i64,

    /// Slow log entries kept
    #[arg(long, default_value = "128")]
    slowlog_max_len: usize,

    /// Latency monitor threshold in milliseconds (0 = off)
    #[arg(long, default_value = "0")]
    latency_monitor_threshold: u64,

    /// Milliseconds a script may run before other clients get -BUSY and
    /// SCRIPT KILL applies (Redis `busy-reply-threshold` / `lua-time-limit`)
    #[arg(long, default_value = "5000")]
    busy_reply_threshold: u64,

    /// Keys remembered for client-side caching (0 = unbounded)
    #[arg(long, default_value = "1000000")]
    tracking_table_max_keys: usize,

    /// Log output: text or json
    #[arg(long, default_value = "text")]
    log_format: String,

    /// Require the REST token for /metrics (yes/no; default: yes when a
    /// token or password is set)
    #[arg(long)]
    metrics_auth: Option<String>,

    /// Redis protocol port
    #[arg(long, default_value = "6379")]
    port: u16,

    /// REST API port (0 = REST API disabled)
    #[arg(long, default_value = "5020")]
    rest_port: u16,

    /// Bearer token for the REST API. Defaults to the RESP password. A
    /// separate token keeps a leaked REST credential from also being the
    /// RESP password.
    #[arg(long, value_name = "TOKEN")]
    rest_token: Option<String>,

    /// Read the REST token from a file (mode 0600 enforced)
    #[arg(long, value_name = "PATH")]
    rest_token_file: Option<PathBuf>,

    /// The ACL user a valid REST Bearer token acts as. Other users
    /// authenticate with `Authorization: Basic` (user:password).
    #[arg(long, default_value = "default", value_name = "USER")]
    rest_token_user: String,

    /// Most concurrent REST connections (0 = no limit)
    #[arg(long, default_value = "1024")]
    rest_max_connections: usize,

    /// Extra Host header names the REST API accepts (comma-separated). On a
    /// loopback bind, localhost/127.0.0.1/::1 are always accepted and any
    /// other Host is refused, which stops DNS rebinding from a browser. On
    /// other binds the check only runs when this is set.
    #[arg(long, value_name = "HOSTS", value_delimiter = ',')]
    rest_allowed_hosts: Vec<String>,

    /// Close an authenticated client idle for this many seconds (0 = never,
    /// like Redis' `timeout 0`)
    #[arg(long, default_value = "0", value_name = "SECS")]
    timeout: u64,

    /// When a password is set, a client must authenticate within this many
    /// seconds of connecting (0 = no limit)
    #[arg(long, default_value = "10", value_name = "SECS")]
    auth_timeout: u64,

    /// Longest a TLS handshake may take, in seconds
    #[arg(long, default_value = "10", value_name = "SECS")]
    tls_handshake_timeout: u64,

    /// Most bytes buffered for one client's pending queries (Redis
    /// `client-query-buffer-limit`)
    #[arg(long, default_value = "1073741824", value_name = "BYTES")]
    client_query_buffer_limit: usize,

    /// Most memory one Lua scripting VM may use, in bytes
    #[arg(long, default_value = "67108864", value_name = "BYTES")]
    lua_memory_limit: usize,

    /// Memory limit (bytes; k/kb/m/mb/g/gb suffixes allowed; 0 = none).
    /// Past it, writes evict keys by --maxmemory-policy or are refused
    #[arg(long, default_value = "0", value_name = "SIZE", value_parser = parse_size)]
    maxmemory: u64,

    /// noeviction, allkeys-lru, volatile-lru, allkeys-lfu, volatile-lfu,
    /// allkeys-random, volatile-random or volatile-ttl
    #[arg(long, default_value = "noeviction", value_parser = parse_policy)]
    maxmemory_policy: solikv_core::store::EvictionPolicy,

    /// Keys sampled per eviction (more = closer to true LRU/LFU, slower)
    #[arg(long, default_value = "5")]
    maxmemory_samples: usize,

    /// Active expiry cycles per second (Redis `hz`)
    #[arg(long, default_value = "10")]
    hz: u32,

    /// Number of logical databases (SELECT 0 … n-1). Cluster mode uses
    /// database 0 only.
    #[arg(long, default_value_t = solikv_engine::dispatch::DEFAULT_DATABASES)]
    databases: u32,

    /// Daemon mode log file (default: <dir>/solikv.log)
    #[arg(long, value_name = "PATH")]
    logfile: Option<PathBuf>,

    /// Number of shards (0 = number of CPU cores). Ignored when --solo is set.
    #[arg(long, default_value = "0")]
    shards: usize,

    /// Solo mode: Redis-shaped single-threaded engine (1 shard, no mutex).
    /// Best for single-core / classic redis-benchmark fights. Uses one Tokio worker.
    #[arg(long, default_value_t = false)]
    solo: bool,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Data directory for persistence files
    #[arg(long, default_value = "data")]
    dir: PathBuf,

    /// Snapshot base filename: the snapshot is {dir}/{name}.rdb (per-shard
    /// {name}-{shard}.rdb files from earlier versions are still loaded)
    #[arg(long, default_value = "dump")]
    dbfilename: String,

    /// Enable append-only file persistence. When on, startup loads the log
    /// (in {dir}/appendonlydir) and not the snapshot, as Redis does.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    appendonly: bool,

    /// AOF fsync policy: always, everysec, no
    #[arg(long, default_value = "everysec")]
    appendfsync: String,

    /// Seconds between background snapshots when the data changed (0 = no
    /// periodic snapshots and no snapshot at shutdown)
    #[arg(long, default_value = "300", value_name = "SECS")]
    save_interval: u64,

    /// Redis `save` points, overriding --save-interval: "3600 1 300 100"
    /// snapshots once 1 change happened within 3600 s, or 100 within 300 s;
    /// "" disables periodic snapshots (and the one at shutdown)
    #[arg(long, value_name = "POINTS")]
    save: Option<String>,

    /// Rewrite the append-only log once it has grown by this percentage since
    /// the last rewrite (0 = only on BGREWRITEAOF)
    #[arg(long, default_value = "100", value_name = "PERCENT")]
    auto_aof_rewrite_percentage: u64,

    /// ... and is at least this large (bytes; k/kb/m/mb/g/gb suffixes allowed)
    #[arg(long, default_value = "64mb", value_name = "SIZE", value_parser = parse_size)]
    auto_aof_rewrite_min_size: u64,

    /// Import a Redis RDB file (dump.rdb) at startup
    #[arg(long, value_name = "PATH")]
    import_redis_rdb: Option<PathBuf>,

    /// Require password for client authentication (RESP AUTH + REST Bearer token)
    #[arg(long, value_name = "PASSWORD")]
    requirepass: Option<String>,

    /// Read password from a file (mode 0600 enforced). Takes precedence over --requirepass.
    #[arg(long, value_name = "PATH")]
    requirepass_file: Option<PathBuf>,

    /// Enable protected mode (reject connections if binding to non-loopback with no password)
    #[arg(long, default_value = "yes")]
    protected_mode: String,

    /// TLS certificate file. With --tls-key, enables TLS on the RESP and
    /// REST ports; it is also this node's client certificate on the links it
    /// opens (--tls-replication, --tls-cluster).
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<String>,

    /// TLS private key file (PKCS#8, PKCS#1 or SEC1 PEM)
    #[arg(long, value_name = "PATH")]
    tls_key: Option<String>,

    /// CA certificate(s) (PEM). Client certificates on RESP/REST are checked
    /// against it (see --tls-auth-clients), and so are the peers of
    /// replication links and the cluster bus.
    #[arg(long, value_name = "PATH", alias = "tls-client-ca")]
    tls_ca_cert: Option<String>,

    /// Client certificates on RESP and REST: yes (required), optional, or no.
    /// Default: yes when --tls-ca-cert is set, as in Redis.
    #[arg(long, value_name = "yes|no|optional")]
    tls_auth_clients: Option<String>,

    /// Replication links (and MIGRATE) use TLS: a replica connects to its
    /// master's TLS port and checks its certificate against --tls-ca-cert.
    #[arg(long, default_value = "no", value_parser = parse_yes_no)]
    tls_replication: bool,

    /// The cluster bus uses TLS, both ways, with a certificate signed by
    /// --tls-ca-cert required of every peer. The bus keeps its HMAC envelope.
    #[arg(long, default_value = "no", value_parser = parse_yes_no)]
    tls_cluster: bool,

    /// Keyspace notification flags (e.g. "KEA" for all events, "" to disable)
    #[arg(long, default_value = "")]
    notify_keyspace_events: String,

    /// Follow a master at startup: "host port" or "host:port" (Redis
    /// `replicaof`). `REPLICAOF` changes it at runtime.
    #[arg(long, value_name = "HOST PORT")]
    replicaof: Option<String>,

    /// Password to authenticate to the master with (`masterauth`).
    #[arg(long)]
    masterauth: Option<String>,

    /// ACL user to authenticate to the master as (`masteruser`).
    #[arg(long)]
    masteruser: Option<String>,

    /// Replication backlog: how much of the stream a reconnecting replica
    /// can resume from without a full resynchronization.
    #[arg(long, default_value = "1mb", value_name = "SIZE", value_parser = parse_size)]
    repl_backlog_size: u64,

    /// Refuse writes from clients while this node is a replica.
    #[arg(long, default_value = "yes", value_parser = parse_yes_no)]
    replica_read_only: bool,

    /// Address a replica announces to its master (`replica-announce-ip`),
    /// when the one the master sees is not the one to reach it on.
    #[arg(long)]
    replica_announce_ip: Option<String>,

    /// Run as daemon in background (kills old instance if running)
    #[arg(short, long)]
    daemon: bool,

    /// Enable cluster mode (Redis Cluster compatible)
    #[arg(long)]
    cluster_enabled: bool,

    /// Address this node announces to the cluster (and to clients, in
    /// MOVED and CLUSTER SLOTS), when the bind address is not it.
    #[arg(long)]
    cluster_announce_ip: Option<String>,

    /// Cluster bus port (0: the data port + 10000).
    #[arg(long, default_value_t = 0)]
    cluster_port: u16,

    /// Milliseconds a node may be unreachable before it is suspected
    /// (`cluster-node-timeout`).
    #[arg(long, default_value_t = 15000)]
    cluster_node_timeout: u64,

    /// Refuse every key while some slot is unassigned or its owner failed
    /// (`cluster-require-full-coverage`).
    #[arg(long, default_value = "yes", value_parser = parse_yes_no)]
    cluster_require_full_coverage: bool,

    /// Cluster dump: export all cluster data to file
    #[arg(long)]
    cluster_dump: Option<String>,

    /// Cluster dump format: binary or jsonl
    #[arg(long, default_value = "jsonl")]
    cluster_dump_format: String,

    /// Cluster restore: import data from dump file
    #[arg(long)]
    cluster_restore: Option<String>,

    /// Cluster connect: initial node for dump/restore (host:port)
    #[arg(long)]
    cluster_connect: Option<String>,

    /// Password `--cluster-dump`/`--cluster-restore` authenticate to the
    /// nodes with (`AUTH`). Before --cluster-bus-secret existed it was also
    /// the bus key; it still is, with a warning, when no bus secret is set.
    #[arg(long)]
    cluster_password: Option<String>,

    /// Read the dump/restore password from a file (mode 0600 enforced)
    #[arg(long)]
    cluster_password_file: Option<PathBuf>,

    /// ACL user dump/restore authenticate as (`AUTH user password`)
    #[arg(long)]
    cluster_user: Option<String>,

    /// The cluster bus key (HMAC-SHA256, at least 32 bytes): every frame
    /// between nodes is signed with it. Separate from any client password,
    /// so a credential handed to an operator tool is not the key to the
    /// topology. Prefer --cluster-bus-secret-file or SOLIKV_CLUSTER_BUS_SECRET.
    #[arg(long, value_name = "SECRET")]
    cluster_bus_secret: Option<String>,

    /// Read the cluster bus key from a file (mode 0600 enforced)
    #[arg(long, value_name = "PATH")]
    cluster_bus_secret_file: Option<PathBuf>,

    /// For cluster dump/restore: send password only to seed node, not to
    /// discovered nodes. `--cluster-password-seed-only=false` sends it to
    /// every node.
    #[arg(
        long,
        default_value_t = true,
        num_args = 0..=1,
        default_missing_value = "true",
        action = clap::ArgAction::Set
    )]
    cluster_password_seed_only: bool,
}

/// The command line with a configuration file's settings in front: the
/// file's flags come first, so the command line's own override them.
fn args_with_config_file() -> Vec<std::ffi::OsString> {
    use clap::CommandFactory;
    let argv: Vec<String> = std::env::args().collect();
    let mut path: Option<PathBuf> = None;
    let mut i = 1;
    while i < argv.len() {
        let a = &argv[i];
        if a == "--config" {
            path = argv.get(i + 1).map(PathBuf::from);
            break;
        }
        if let Some(p) = a.strip_prefix("--config=") {
            path = Some(PathBuf::from(p));
            break;
        }
        if i == 1 && !a.starts_with('-') {
            path = Some(PathBuf::from(a));
            break;
        }
        i += 1;
    }
    let Some(path) = path else {
        return argv.into_iter().map(Into::into).collect();
    };
    let cmd = Args::command();
    let known = |flag: &str| cmd.get_arguments().any(|a| a.get_long() == Some(flag));
    let file_flags = match solikv_server::config_file::load(&path, &known) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let mut out: Vec<std::ffi::OsString> = vec![argv[0].clone().into()];
    out.extend(file_flags.into_iter().map(Into::into));
    out.extend(argv.into_iter().skip(1).map(Into::into));
    out
}

fn main() {
    solikv_core::memory::enable();
    let args = Args::parse_from(args_with_config_file());

    if args.solo && args.cluster_enabled {
        eprintln!("ERROR: --solo is incompatible with --cluster-enabled");
        std::process::exit(1);
    }

    // Solo mode pins the whole server to one OS worker thread so the unlocked
    // store is only ever touched by a single thread (Redis-shaped).
    let mut builder = if args.solo {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(1).thread_name("solikv-solo");
        b
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.thread_name("solikv");
        b
    };
    let rt = builder
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");
    // Run the server *on* a runtime worker, not on this thread. `block_on`
    // polls its future on the calling thread, which in solo mode is a second
    // thread next to the single worker — and the shutdown save ran there,
    // walking the unlocked solo store while connections were still served.
    let server = rt.spawn(async_main(args));
    if let Err(e) = rt.block_on(server) {
        eprintln!("ERROR: server task failed: {e}");
        std::process::exit(1);
    }
}

/// Best-effort absolute rendering of a path, for error messages. `--dir` defaults
/// to the relative "data", so a bare path tells the user nothing about where the
/// server actually tried to write.
fn abs_display(p: &std::path::Path) -> String {
    std::path::absolute(p)
        .unwrap_or_else(|_| p.to_path_buf())
        .display()
        .to_string()
}

async fn async_main(args: Args) {
    let pidfile = args.dir.join("solikv.pid");

    if args.daemon {
        if pidfile.exists() {
            if let Ok(pid_str) = fs::read_to_string(&pidfile) {
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    if pid > 0 {
                        // /proc/<pid>/exe is a symlink to the running binary.
                        // Resolve both paths to canonical form before comparing
                        // so we don't kill an unrelated PID that happens to be
                        // recycled into the pidfile.
                        let exe_path = PathBuf::from(format!("/proc/{}/exe", pid));
                        let target = fs::read_link(&exe_path).and_then(|p| p.canonicalize()).ok();
                        let current = std::env::current_exe().and_then(|p| p.canonicalize()).ok();
                        let is_solikv = matches!((target, current), (Some(t), Some(c)) if t == c);
                        if is_solikv {
                            println!("Killing old solikv process (PID: {})...", pid);
                            let _ =
                                kill(Pid::from_raw(pid as i32), nix::sys::signal::Signal::SIGTERM);
                        } else {
                            println!("PID {} is not a solikv process, skipping kill", pid);
                        }
                        let _ = fs::remove_file(&pidfile);
                    }
                }
            }
        }

        // Not a fixed path in /tmp: any local user could pre-create
        // /tmp/solikv.log as a symlink and have the daemon append to a file
        // of their choosing. The data directory is ours; the file is 0600
        // and opened without following symlinks.
        if let Err(e) = solikv_persist::fsutil::create_private_dir_all(&args.dir) {
            eprintln!("ERROR: cannot create {}: {}", abs_display(&args.dir), e);
            std::process::exit(1);
        }
        let log_file = args
            .logfile
            .clone()
            .unwrap_or_else(|| args.dir.join("solikv.log"));
        let file = solikv_persist::fsutil::open_private_append(&log_file).unwrap_or_else(|e| {
            eprintln!(
                "ERROR: cannot open log file {}: {}",
                abs_display(&log_file),
                e
            );
            std::process::exit(1);
        });

        #[allow(clippy::zombie_processes)]
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(
                std::env::args()
                    .skip(1)
                    .filter(|a| *a != "-d" && *a != "--daemon"),
            )
            .stdout(file.try_clone().expect("Failed to clone log"))
            .stderr(file)
            .spawn()
            .expect("Failed to spawn daemon");

        let pid = child.id();
        solikv_persist::fsutil::write_private(&pidfile, pid.to_string())
            .expect("Failed to write pidfile");
        println!("Started daemon, PID: {}", pid);
        return;
    }

    // Warnings found before logging is set up. Printed once it is, or to
    // stderr by the dump/restore paths that exit before that point.
    let mut early_warnings: Vec<&str> = Vec::new();

    let cluster_password: Option<String> = if let Some(file_path) = &args.cluster_password_file {
        let pw = read_secret_file(file_path).unwrap_or_else(|e| {
            eprintln!(
                "ERROR: failed to read cluster password file {:?}: {}",
                file_path, e
            );
            std::process::exit(1);
        });
        Some(pw)
    } else if let Some(pw) = &args.cluster_password {
        early_warnings.push(
            "Cluster password via CLI flag is visible in /proc/cmdline. Consider using \
             --cluster-password-file or SOLIKV_CLUSTER_PASSWORD env var.",
        );
        Some(pw.clone())
    } else {
        std::env::var("SOLIKV_CLUSTER_PASSWORD").ok()
    };

    // Handle cluster dump/restore operations (run standalone, don't start server)
    if args.cluster_dump.is_some() || args.cluster_restore.is_some() {
        for w in &early_warnings {
            eprintln!("WARNING: {w}");
        }
    }
    if let Some(dump_path) = &args.cluster_dump {
        if args.cluster_connect.is_none() {
            eprintln!("Error: --cluster-connect required for cluster dump");
            std::process::exit(1);
        }
        let result = cluster_dump::dump_cluster(
            std::path::Path::new(dump_path),
            &args.cluster_dump_format,
            args.cluster_connect.as_ref().unwrap(),
            args.cluster_user.as_deref(),
            cluster_password.as_deref(),
            if args.cluster_password_seed_only {
                None
            } else {
                cluster_password.as_deref()
            },
        );
        match result {
            Ok((keys, nodes)) => {
                println!("Cluster dump complete: {} keys from {} nodes", keys, nodes);
            }
            Err(e) => {
                eprintln!("Cluster dump failed: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }

    if let Some(restore_path) = &args.cluster_restore {
        if args.cluster_connect.is_none() {
            eprintln!("Error: --cluster-connect required for cluster restore");
            std::process::exit(1);
        }
        let result = cluster_dump::restore_cluster(
            std::path::Path::new(restore_path),
            args.cluster_connect.as_ref().unwrap(),
            args.cluster_user.as_deref(),
            cluster_password.as_deref(),
            if args.cluster_password_seed_only {
                None
            } else {
                cluster_password.as_deref()
            },
        );
        match result {
            Ok((success, errors)) => {
                println!(
                    "Cluster restore complete: {} success, {} errors",
                    success, errors
                );
            }
            Err(e) => {
                eprintln!("Cluster restore failed: {}", e);
                std::process::exit(1);
            }
        }
        return;
    }

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    if args.log_format.eq_ignore_ascii_case("json") {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    for w in &early_warnings {
        tracing::warn!("{}", w);
    }

    let num_shards = if args.solo {
        1
    } else if args.shards == 0 {
        num_cpus().max(1)
    } else {
        args.shards
    };

    let fsync_policy = match args.appendfsync.as_str() {
        "always" => FsyncPolicy::Always,
        "no" => FsyncPolicy::No,
        _ => FsyncPolicy::Everysec,
    };

    let password: Option<Arc<String>> = if let Some(file_path) = &args.requirepass_file {
        let pw = read_secret_file(file_path).unwrap_or_else(|e| {
            eprintln!("ERROR: failed to read password file {:?}: {}", file_path, e);
            std::process::exit(1);
        });
        Some(Arc::new(pw))
    } else if let Some(pw) = &args.requirepass {
        tracing::warn!("Password supplied via CLI flag is visible in /proc/cmdline. Consider using --requirepass-file or SOLIKV_REQUIREPASS env var.");
        Some(Arc::new(pw.clone()))
    } else if let Ok(env_pw) = std::env::var("SOLIKV_REQUIREPASS") {
        Some(Arc::new(env_pw))
    } else {
        None
    };

    let rest_token: Option<Arc<String>> = if let Some(file_path) = &args.rest_token_file {
        let token = read_secret_file(file_path).unwrap_or_else(|e| {
            eprintln!(
                "ERROR: failed to read REST token file {:?}: {}",
                file_path, e
            );
            std::process::exit(1);
        });
        Some(Arc::new(token))
    } else if let Some(token) = &args.rest_token {
        tracing::warn!("REST token supplied via CLI flag is visible in /proc/cmdline. Consider using --rest-token-file or SOLIKV_REST_TOKEN env var.");
        Some(Arc::new(token.clone()))
    } else if let Ok(token) = std::env::var("SOLIKV_REST_TOKEN") {
        Some(Arc::new(token))
    } else {
        password.clone()
    };

    let is_loopback = args.bind == "127.0.0.1" || args.bind == "::1" || args.bind == "localhost";
    // Protected mode is decided once the ACL file is loaded (it may give
    // the `default` user a password): see below.
    let protected = args.protected_mode.to_lowercase() != "no";

    let tls: Option<solikv_server::tls::Tls> = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let auth_clients = match args.tls_auth_clients.as_deref() {
                Some(v) => match solikv_server::tls::ClientAuth::parse(v) {
                    Ok(a) => Some(a),
                    Err(e) => {
                        eprintln!("ERROR: --tls-auth-clients: {e}");
                        std::process::exit(1);
                    }
                },
                None => None,
            };
            let opts = solikv_server::tls::TlsOptions {
                cert: cert.into(),
                key: key.into(),
                ca: args.tls_ca_cert.as_ref().map(PathBuf::from),
                auth_clients,
                replication: args.tls_replication,
                cluster: args.tls_cluster,
            };
            match solikv_server::tls::Tls::load(&opts) {
                Ok(t) => {
                    tracing::info!(
                        client_certificates = ?auth_clients.unwrap_or(if opts.ca.is_some() {
                            solikv_server::tls::ClientAuth::Required
                        } else {
                            solikv_server::tls::ClientAuth::No
                        }),
                        replication = t.replication,
                        cluster_bus = t.cluster,
                        "TLS enabled"
                    );
                    Some(t)
                }
                Err(e) => {
                    eprintln!("ERROR: TLS: {e}");
                    std::process::exit(1);
                }
            }
        }
        (None, None) => {
            if args.tls_ca_cert.is_some()
                || args.tls_auth_clients.is_some()
                || args.tls_replication
                || args.tls_cluster
            {
                eprintln!(
                    "ERROR: TLS options were given without --tls-cert and --tls-key; refusing \
                     to start without the encryption they ask for."
                );
                std::process::exit(1);
            }
            tracing::info!("TLS disabled (no --tls-cert/--tls-key provided)");
            None
        }
        _ => {
            eprintln!("ERROR: --tls-cert and --tls-key go together");
            std::process::exit(1);
        }
    };
    let tls_config: Option<rustls::ServerConfig> = tls.as_ref().map(|t| (*t.server).clone());

    if args.solo {
        tracing::info!(
            "SoliKV starting in SOLO mode (1 shard, no mutex, 1 worker), RESP on port {}, REST on port {}",
            args.port,
            args.rest_port
        );
    } else {
        tracing::info!(
            "SoliKV starting with {} shards, RESP on port {}, REST on port {}",
            num_shards,
            args.port,
            args.rest_port
        );
    }
    if password.is_some() {
        tracing::info!("Authentication enabled");
    } else {
        tracing::info!("Authentication disabled (no password set)");
    }

    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());

    // --- Cluster mode: the node and its bus (started once the engine exists) ---
    let cluster_setup = if args.cluster_enabled {
        let announce_ip = args.cluster_announce_ip.clone().unwrap_or_else(|| {
            if args.bind == "0.0.0.0" || args.bind == "127.0.0.1" {
                "127.0.0.1".to_string()
            } else {
                args.bind.clone()
            }
        });
        // The bus secret. Required, and checked before anything is bound:
        // the bus decides membership, slot ownership and failovers, so an
        // unauthenticated one lets a stranger decide all three.
        let bus_key: Option<String> = if let Some(path) = &args.cluster_bus_secret_file {
            Some(read_secret_file(path).unwrap_or_else(|e| {
                eprintln!("ERROR: failed to read cluster bus secret file {path:?}: {e}");
                std::process::exit(1);
            }))
        } else if let Some(s) = &args.cluster_bus_secret {
            tracing::warn!(
                "Cluster bus secret via CLI flag is visible in /proc/cmdline. Consider \
                 --cluster-bus-secret-file or SOLIKV_CLUSTER_BUS_SECRET."
            );
            Some(s.clone())
        } else if let Ok(s) = std::env::var("SOLIKV_CLUSTER_BUS_SECRET") {
            Some(s)
        } else if let Some(pw) = cluster_password.clone() {
            // Deprecated: the dump/restore password doubling as the bus key.
            tracing::warn!(
                "Using the cluster password as the cluster bus secret. This is deprecated: \
                 the same string then authenticates clients for dump/restore and signs the \
                 topology. Set --cluster-bus-secret-file (or SOLIKV_CLUSTER_BUS_SECRET)."
            );
            Some(pw)
        } else {
            None
        };
        let bus_secret = match bus_key.as_deref() {
            Some(key) => match solikv_cluster::auth::ClusterSecret::new(key) {
                Ok(secret) => secret,
                Err(e) => {
                    eprintln!("ERROR: cluster mode needs a usable bus secret: {e}");
                    eprintln!("       set --cluster-bus-secret-file or SOLIKV_CLUSTER_BUS_SECRET");
                    std::process::exit(1);
                }
            },
            None => {
                eprintln!(
                    "ERROR: --cluster-enabled requires a cluster bus secret: the bus decides \
                     cluster membership and slot ownership, and an unauthenticated bus lets \
                     anyone who can reach the port decide both."
                );
                eprintln!("       set --cluster-bus-secret-file or SOLIKV_CLUSTER_BUS_SECRET");
                std::process::exit(1);
            }
        };
        let cport = match args.cluster_port {
            0 => match args
                .port
                .checked_add(solikv_cluster::CLUSTER_BUS_PORT_OFFSET)
            {
                Some(p) => p,
                None => {
                    eprintln!("ERROR: port + 10000 is not a valid bus port; set --cluster-port");
                    std::process::exit(1);
                }
            },
            p => p,
        };
        let bus_bind_ip = if args.bind == "0.0.0.0" {
            "0.0.0.0".to_string()
        } else {
            announce_ip.clone()
        };
        if !args.tls_cluster && !is_loopback {
            tracing::warn!(
                "The cluster bus is authenticated but not encrypted: topology, failover votes \
                 and relayed PUBLISH payloads cross the network in cleartext. Use \
                 --tls-cluster yes on untrusted networks."
            );
        }
        let bus_bind: std::net::SocketAddr = format!("{}:{}", bus_bind_ip, cport)
            .parse()
            .unwrap_or_else(|e| {
                eprintln!(
                    "ERROR: cannot form a cluster bus address from {bus_bind_ip}:{cport}: {e}"
                );
                std::process::exit(1);
            });
        let cluster = Arc::new(ClusterManager::new(
            announce_ip,
            args.port,
            cport,
            solikv_cluster::ClusterConfig {
                node_timeout_ms: args.cluster_node_timeout.max(100),
                require_full_coverage: args.cluster_require_full_coverage,
                config_path: Some(args.dir.join("cluster.json")),
            },
        ));
        tracing::info!(node_id = %cluster.node_id(), %bus_bind, "cluster mode enabled");
        Some((cluster, bus_secret, bus_bind))
    } else {
        None
    };
    let cluster_manager: Option<Arc<ClusterManager>> =
        cluster_setup.as_ref().map(|(c, _, _)| c.clone());

    // Parse keyspace notification flags
    let notify_flags = Arc::new(std::sync::atomic::AtomicU16::new(
        solikv_engine::dispatch::parse_notify_flags(&args.notify_keyspace_events),
    ));
    if notify_flags.load(std::sync::atomic::Ordering::Relaxed) != 0 {
        tracing::info!(
            "Keyspace notifications enabled: {}",
            args.notify_keyspace_events
        );
    }

    // Create shards with notification context (pubsub + flags) for expiry notifications.
    // Solo mode: unlocked single shard (Redis-shaped hot path).
    let shards = Arc::new(if args.solo {
        solikv_engine::ShardManager::solo(pubsub.clone(), notify_flags.clone())
    } else {
        solikv_engine::ShardManager::with_notifications(
            num_shards,
            pubsub.clone(),
            notify_flags.clone(),
        )
    });

    // --- Engine and persistence ---
    let persistence = Arc::new(solikv_engine::Persistence::new(
        solikv_engine::PersistConfig {
            dir: args.dir.clone(),
            dbfilename: args.dbfilename.clone(),
            appendonly: args.appendonly,
            fsync: fsync_policy,
            auto_rewrite_percentage: args.auto_aof_rewrite_percentage,
            auto_rewrite_min_size: args.auto_aof_rewrite_min_size,
            save: match &args.save {
                Some(points) => match solikv_engine::PersistConfig::parse_save(points) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("ERROR: --save {:?}: {}", points, e);
                        std::process::exit(1);
                    }
                },
                None if args.save_interval > 0 => vec![(args.save_interval, 1)],
                None => Vec::new(),
            },
        },
    ));
    let engine = solikv_engine::CommandEngine::new(shards.clone(), pubsub.clone())
        .with_notify_flags(notify_flags)
        .with_lua_memory_limit(args.lua_memory_limit)
        .with_databases(args.databases)
        .with_maxmemory(
            args.maxmemory,
            args.maxmemory_policy,
            args.maxmemory_samples,
        )
        .with_persistence(persistence.clone());
    let engine = Arc::new(engine);
    engine.init_self_ref(Arc::downgrade(&engine));
    engine.set_tcp_port(args.port);
    if let Err(e) = engine.set_debug_mode(&args.enable_debug_command) {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    }
    engine.set_maxclients(args.maxclients);
    engine.set_idle_timeout(args.timeout);
    engine.set_busy_threshold_ms(args.busy_reply_threshold);
    {
        use std::sync::atomic::Ordering::Relaxed;
        let st = engine.stats();
        st.slowlog_threshold_us
            .store(args.slowlog_log_slower_than, Relaxed);
        st.slowlog_max_len.store(args.slowlog_max_len, Relaxed);
        st.latency_threshold_ms
            .store(args.latency_monitor_threshold, Relaxed);
        engine
            .tracking()
            .max_keys
            .store(args.tracking_table_max_keys, Relaxed);
    }
    solikv_engine::stats::calibrate();
    let config_path = args.config.clone().or(args.config_positional.clone());
    engine.set_config_file(
        config_path
            .as_ref()
            .map(|p| std::path::absolute(p).unwrap_or(p.clone())),
    );
    for (k, v) in [
        ("bind", args.bind.clone()),
        ("loglevel", args.log_level.clone()),
        (
            "logfile",
            args.logfile
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        ),
        ("protected-mode", args.protected_mode.clone()),
        (
            "cluster-enabled",
            if args.cluster_enabled { "yes" } else { "no" }.to_string(),
        ),
        ("rest-port", args.rest_port.to_string()),
        (
            "aclfile",
            args.aclfile
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        ),
        (
            "daemonize",
            if args.daemon { "yes" } else { "no" }.to_string(),
        ),
        ("io-threads", "1".to_string()),
    ] {
        engine.set_static_config(k, v);
    }
    // `requirepass` is the default user's password.
    if let Some(pw) = &password {
        engine.acl().set_requirepass(Some(pw.as_str()));
    }
    if let Some(path) = &args.aclfile {
        engine.acl().set_file(Some(path.clone()));
        if path.exists() {
            if let Err(e) = engine.acl().load_file() {
                eprintln!("ERROR: {e}");
                std::process::exit(1);
            }
            tracing::info!("Loaded ACL users from {}", abs_display(path));
        }
    }
    // Protected mode: a non-loopback bind where anyone who connects is the
    // unrestricted `default` user without a password is refused — on every
    // port this process opens (RESP, REST, and the bus, which needs its own
    // secret regardless).
    if !is_loopback && protected && engine.acl().default_auto_auth() {
        eprintln!("ERROR: SoliKV is running in protected mode because no password is set.");
        eprintln!("To disable protected mode, either set a password with --requirepass PASSWORD");
        eprintln!("(or give the default user one in --aclfile), or pass --protected-mode no");
        eprintln!("(NOT RECOMMENDED for exposed deployments).");
        std::process::exit(1);
    }
    // Throughput and peak memory for INFO, sampled ten times a second.
    {
        let engine_stats = engine.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                ticker.tick().await;
                engine_stats.stats().sample(
                    engine_stats.clients().commands_processed(),
                    solikv_core::memory::used() as u64,
                );
            }
        });
    }

    // --- Import Redis RDB if requested ---
    if let Some(ref redis_rdb_path) = args.import_redis_rdb {
        tracing::info!("Importing Redis RDB from {:?}...", redis_rdb_path);
        match solikv_persist::redis_rdb::import_redis_rdb_dbs(redis_rdb_path, num_shards) {
            Ok((shard_data, stats)) => {
                // The importer buckets by cluster slot; the engine routes by
                // its own hash, so every key is re-routed (bucket i is not
                // shard i — keys used to land where no lookup found them).
                let total = solikv_engine::persistence::insert_routed(
                    &shards,
                    shard_data.into_iter().flatten(),
                );
                persistence.mark_needs_rewrite();
                tracing::info!("Redis RDB import complete: {stats} ({total} keys loaded into {num_shards} shards)");
            }
            Err(e) => {
                tracing::error!(
                    "Failed to import Redis RDB from {:?}: {}",
                    redis_rdb_path,
                    e
                );
                std::process::exit(1);
            }
        }
    }

    // --- Load the snapshot or the append-only log, then start the log ---
    match engine.load_persisted() {
        Ok(report) => tracing::info!(
            "Loaded {} keys from {} ({} log records) in {}",
            report.keys,
            report.source,
            report.log_records,
            abs_display(&args.dir)
        ),
        Err(e) => {
            // Starting anyway would serve a partial dataset and then append
            // new writes after the damage. Redis refuses too.
            tracing::error!(
                "Cannot load persisted data from {}: {}. Refusing to start: repair or move \
                 the damaged file (a log cut off mid-record is loaded automatically; this \
                 is not one).",
                abs_display(&args.dir),
                e
            );
            std::process::exit(1);
        }
    }

    // --- Replication ---
    engine
        .repl()
        .set_backlog_size(args.repl_backlog_size as usize);
    engine.set_replica_read_only(args.replica_read_only);
    let replication = solikv_server::replication::Replication::install(
        engine.clone(),
        solikv_server::replication::ReplConfig {
            masterauth: args.masterauth.clone(),
            masteruser: args.masteruser.clone(),
            listening_port: args.port,
            announce_ip: args.replica_announce_ip.clone(),
            dir: args.dir.clone(),
            tls: tls.as_ref().and_then(|t| t.replication_client()),
        },
    );
    solikv_server::replication::spawn_master_pinger(engine.clone());

    // --- Cluster: attach the node to this server, start its bus and cron ---
    if let Some((cluster, bus_secret, bus_bind)) = cluster_setup {
        engine.enable_slot_index();
        let bus_tls = tls.as_ref().filter(|t| t.cluster).and_then(|t| {
            Some(solikv_cluster::gossip::BusTls {
                connector: t.client.as_ref()?.connector(),
                acceptor: tokio_rustls::TlsAcceptor::from(t.bus_server.clone()?),
            })
        });
        let bus = solikv_cluster::Bus::with_tls(bus_secret.clone(), bus_tls);
        cluster.attach(
            bus.clone(),
            Arc::new(solikv_server::cluster_host::ServerClusterHost::new(
                engine.clone(),
                replication.clone(),
            )),
        );
        {
            let c = cluster.clone();
            engine.set_publish_hook(Box::new(move |ch, msg, shard| c.publish(ch, msg, shard)));
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        if let Err(e) = solikv_cluster::gossip::start_bus_server(
            bus_bind,
            bus_secret,
            cluster.my_id_handle(),
            tx,
            bus,
        )
        .await
        {
            eprintln!("ERROR: cluster bus failed to bind {bus_bind}: {e}");
            std::process::exit(1);
        }
        let c = cluster.clone();
        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                c.handle(m);
            }
        });
        let c = cluster.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                c.cron();
            }
        });
        // A node that was a replica when it stopped follows its master again.
        if let Some((ip, port)) = cluster.my_master_addr() {
            use solikv_engine::replication::ReplController;
            let _ = replication.replicaof(ip, port);
        }
    } else if let Some(spec) = &args.replicaof {
        match parse_host_port(spec) {
            Some((host, port)) => {
                use solikv_engine::replication::ReplController;
                if let Err(e) = replication.replicaof(host, port) {
                    eprintln!("ERROR: replicaof: {e}");
                    std::process::exit(1);
                }
            }
            None => {
                eprintln!("ERROR: --replicaof expects \"host port\" or host:port, got {spec:?}");
                std::process::exit(1);
            }
        }
    }

    // --- Active expiry: keys expire whether or not anything reads them ---
    solikv_engine::shard::spawn_active_expiry(shards.clone(), args.hz);

    // --- Periodic snapshots and automatic log rewrites ---
    {
        let engine_bg = engine.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let started_unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            loop {
                ticker.tick().await;
                // Channels whose last subscriber left.
                engine_bg.pubsub.cleanup();
                let p = engine_bg.persistence().clone();
                if p.should_auto_rewrite() {
                    tracing::info!("Append-only log has grown; rewriting it");
                    if let Err(e) = p.bgrewrite(&engine_bg.shards) {
                        tracing::warn!("Automatic log rewrite not started: {}", e);
                    }
                }
                // Save points, timed from the last snapshot of any kind
                // (SAVE and BGSAVE count, as in Redis).
                let last = p
                    .stats
                    .rdb_last_save_time
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .max(started_unix);
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let dirty = engine_bg.dirty();
                let saved_at = p
                    .stats
                    .rdb_dirty_at_last_save
                    .load(std::sync::atomic::Ordering::Relaxed);
                if p.config()
                    .save_due(now.saturating_sub(last), dirty.saturating_sub(saved_at))
                {
                    if let Err(e) = p.bgsave(&engine_bg.shards, dirty) {
                        tracing::debug!("Periodic snapshot skipped: {}", e);
                    }
                }
            }
        });
    }

    let resp_addr = format!("{}:{}", args.bind, args.port);
    let rest_addr = format!("{}:{}", args.bind, args.rest_port);

    tracing::info!("Starting RESP server on {}", resp_addr);
    if args.rest_port != 0 {
        tracing::info!("Starting REST server on {}", rest_addr);
    }

    let engine_resp = engine.clone();
    let pubsub_resp = pubsub.clone();
    let password_resp = password.clone();
    let cluster_resp = cluster_manager.clone();
    let tls_config_resp = tls_config.clone();
    let secs = |n: u64| (n > 0).then(|| std::time::Duration::from_secs(n));
    let limits = resp_server::ConnLimits {
        handshake_timeout: std::time::Duration::from_secs(args.tls_handshake_timeout.max(1)),
        auth_timeout: secs(args.auth_timeout),
        idle_timeout: secs(args.timeout),
        query_buffer_limit: args.client_query_buffer_limit,
        outbound_tls: tls.as_ref().and_then(|t| t.replication_client()),
    };
    let resp_handle = tokio::spawn(async move {
        if let Err(e) = resp_server::run_with_limits(
            &resp_addr,
            engine_resp,
            pubsub_resp,
            password_resp,
            cluster_resp,
            tls_config_resp,
            limits,
        )
        .await
        {
            tracing::error!("RESP server error: {}", e);
        }
    });

    let rest_config = rest_server::RestConfig {
        token: rest_token.clone(),
        token_user: Some(args.rest_token_user.clone()),
        max_connections: args.rest_max_connections,
        tls: tls_config.clone(),
        allowed_hosts: if is_loopback || !args.rest_allowed_hosts.is_empty() {
            let mut hosts: Vec<String> = args.rest_allowed_hosts.clone();
            if is_loopback {
                hosts.extend(rest_server::LOOPBACK_HOSTS.iter().map(|h| h.to_string()));
            }
            Some(hosts)
        } else {
            None
        },
        handshake_timeout: std::time::Duration::from_secs(args.tls_handshake_timeout.max(1)),
        metrics_auth: match args.metrics_auth.as_deref() {
            Some(v) => matches!(v.to_ascii_lowercase().as_str(), "yes" | "true" | "1"),
            None => rest_token.is_some(),
        },
    };
    let engine_rest = engine.clone();
    let rest_enabled = args.rest_port != 0;
    let rest_handle = tokio::spawn(async move {
        if !rest_enabled {
            tracing::info!("REST API disabled (--rest-port 0)");
            return std::future::pending::<()>().await;
        }
        if let Err(e) = rest_server::run_with(&rest_addr, engine_rest, rest_config).await {
            tracing::error!("REST server error: {}", e);
        }
    });

    // SIGTERM (docker stop, systemd, the daemon restart) and SIGINT shut down
    // gracefully; SIGHUP no longer kills the process. Only Ctrl-C used to
    // save: every other stop lost whatever the last snapshot did not hold.
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
    let shutdown_cmd = engine.shutdown_requested();
    let mut resp_handle = resp_handle;
    let mut rest_handle = rest_handle;
    let reason = loop {
        tokio::select! {
            _ = &mut resp_handle => break "RESP server stopped",
            _ = &mut rest_handle => break "REST server stopped",
            _ = sigterm.recv() => break "SIGTERM",
            _ = sigint.recv() => break "SIGINT",
            _ = sighup.recv() => tracing::info!("SIGHUP received and ignored"),
            _ = shutdown_cmd.notified() => break "SHUTDOWN",
        }
    };

    tracing::info!("Shutting down ({})", reason);
    // SHUTDOWN already froze the shards, synced the log and saved.
    if reason != "SHUTDOWN" {
        match engine.shutdown(None) {
            Ok(()) => tracing::info!("Data made durable in {}", abs_display(&args.dir)),
            Err(e) => tracing::error!(
                "Final save to {} failed: {} — data since the last successful save may be lost",
                abs_display(&args.dir),
                e
            ),
        }
    }

    // Cluster configuration: saved on every change; once more on the way out.
    if let Some(ref cluster) = cluster_manager {
        cluster.save_now();
    }

    // The pidfile names this process only in daemon mode; leave anyone
    // else's alone.
    let pidfile = args.dir.join("solikv.pid");
    if fs::read_to_string(&pidfile).is_ok_and(|p| p.trim() == std::process::id().to_string()) {
        let _ = fs::remove_file(&pidfile);
    }
    std::process::exit(0);
}

/// `64mb`, `1g`, `4096` → bytes.
fn parse_policy(s: &str) -> Result<solikv_core::store::EvictionPolicy, String> {
    solikv_core::store::EvictionPolicy::parse(s)
        .ok_or_else(|| format!("unknown maxmemory policy '{s}'"))
}

fn parse_yes_no(s: &str) -> Result<bool, String> {
    match s.to_ascii_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        other => Err(format!("expected yes or no, got '{other}'")),
    }
}

/// `host port` or `host:port`.
fn parse_host_port(s: &str) -> Option<(String, u16)> {
    let s = s.trim();
    let (h, p) = match s.split_once(char::is_whitespace) {
        Some((h, p)) => (h.trim(), p.trim()),
        None => s.rsplit_once(':')?,
    };
    Some((h.to_string(), p.parse().ok()?))
}

fn parse_size(s: &str) -> Result<u64, String> {
    let lower = s.trim().to_ascii_lowercase();
    let (num, mult) = [
        ("gb", 1u64 << 30),
        ("g", 1 << 30),
        ("mb", 1 << 20),
        ("m", 1 << 20),
        ("kb", 1 << 10),
        ("k", 1 << 10),
        ("b", 1),
    ]
    .iter()
    .find_map(|(suf, m)| lower.strip_suffix(suf).map(|n| (n.to_string(), *m)))
    .unwrap_or((lower.clone(), 1));
    num.trim()
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .ok_or_else(|| format!("invalid size '{}'", s))
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Read a secret from a file with hardening:
/// - Open with O_NOFOLLOW so a symlink in place of the file is rejected.
/// - Refuse if any group/world bit is set (mode must be ≤ 0o600).
/// - Strip trailing whitespace/newlines, return the secret as a String.
fn read_secret_file(path: &std::path::Path) -> std::io::Result<String> {
    use std::io::Read;
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;

    let meta = f.metadata()?;
    if !meta.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "secret file must be a regular file",
        ));
    }
    if meta.mode() & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "secret file permissions must be 0600 (no group/world access)",
        ));
    }

    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    Ok(buf.trim_end_matches(['\n', '\r', ' ', '\t']).to_string())
}
