use mimalloc::MiMalloc;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::Parser;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use nix::sys::signal::kill;
use nix::unistd::Pid;

use solikv_persist::{AofPersistence, FsyncPolicy};

use solikv_cluster::{generate_stable_node_id, ClusterManager, GossipState};
use solikv_server::cluster_dump;
use solikv_server::resp_server;
use solikv_server::rest_server;

#[derive(Parser, Debug)]
#[command(
    name = "solikv",
    about = "SoliKV - High-Performance In-Memory Database"
)]
struct Args {
    /// Redis protocol port
    #[arg(long, default_value = "6379")]
    port: u16,

    /// REST API port
    #[arg(long, default_value = "5020")]
    rest_port: u16,

    /// Number of shards (0 = number of CPU cores)
    #[arg(long, default_value = "0")]
    shards: usize,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Data directory for persistence files
    #[arg(long, default_value = "data")]
    dir: PathBuf,

    /// RDB snapshot base filename (files will be {name}-{shard}.rdb)
    #[arg(long, default_value = "dump")]
    dbfilename: String,

    /// Enable append-only file persistence
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    appendonly: bool,

    /// AOF fsync policy: always, everysec, no
    #[arg(long, default_value = "everysec")]
    appendfsync: String,

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

    /// TLS certificate file (enables TLS on RESP and REST if set)
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<String>,

    /// TLS private key file
    #[arg(long, value_name = "PATH")]
    tls_key: Option<String>,

    /// TLS client CA certificate for client certificate verification
    #[arg(long, value_name = "PATH")]
    tls_client_ca: Option<String>,

    /// Keyspace notification flags (e.g. "KEA" for all events, "" to disable)
    #[arg(long, default_value = "")]
    notify_keyspace_events: String,

    /// Run as daemon in background (kills old instance if running)
    #[arg(short, long)]
    daemon: bool,

    /// Enable cluster mode (Redis Cluster compatible)
    #[arg(long)]
    cluster_enabled: bool,

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

    /// Cluster password for dump/restore
    #[arg(long)]
    cluster_password: Option<String>,

    /// Read cluster password from a file
    #[arg(long)]
    cluster_password_file: Option<PathBuf>,

    /// For cluster dump/restore: send password only to seed node, not to discovered nodes
    #[arg(long, default_value = "true")]
    cluster_password_seed_only: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

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
                        let target = fs::read_link(&exe_path)
                            .and_then(|p| p.canonicalize())
                            .ok();
                        let current = std::env::current_exe()
                            .and_then(|p| p.canonicalize())
                            .ok();
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

        let log_file = PathBuf::from("/tmp/solikv.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)
            .expect("Failed to open log file");

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
        fs::write(&pidfile, pid.to_string()).expect("Failed to write pidfile");
        println!("Started daemon, PID: {}", pid);
        return;
    }

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
        tracing::warn!("Cluster password via CLI flag is visible in /proc/cmdline. Consider using --cluster-password-file or SOLIKV_CLUSTER_PASSWORD env var.");
        Some(pw.clone())
    } else if let Ok(env_pw) = std::env::var("SOLIKV_CLUSTER_PASSWORD") {
        Some(env_pw)
    } else {
        None
    };

    // Handle cluster dump/restore operations (run standalone, don't start server)
    if let Some(dump_path) = &args.cluster_dump {
        if args.cluster_connect.is_none() {
            eprintln!("Error: --cluster-connect required for cluster dump");
            std::process::exit(1);
        }
        let result = cluster_dump::dump_cluster(
            std::path::Path::new(dump_path),
            &args.cluster_dump_format,
            args.cluster_connect.as_ref().unwrap(),
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

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)),
        )
        .init();

    let num_shards = if args.shards == 0 {
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
            eprintln!(
                "ERROR: failed to read password file {:?}: {}",
                file_path, e
            );
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

    let is_loopback = args.bind == "127.0.0.1" || args.bind == "::1" || args.bind == "localhost";
    if !is_loopback && password.is_none() && args.protected_mode.to_lowercase() != "no" {
        eprintln!("ERROR: SoliKV is running in protected mode because no password is set.");
        eprintln!("To disable protected mode, either set a password with --requirepass PASSWORD");
        eprintln!("or pass --protected-mode no (NOT RECOMMENDED for exposed deployments).");
        std::process::exit(1);
    }

    let tls_config = match (&args.tls_cert, &args.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert = std::fs::read(cert_path).expect("Failed to read TLS certificate");
            let key = std::fs::read(key_path).expect("Failed to read TLS private key");
            let certs = pem::parse_many(&cert).expect("Failed to parse TLS certificate");
            let keys = pem::parse_many(&key).expect("Failed to parse TLS private key");
            let certs: Vec<rustls::pki_types::CertificateDer> = certs
                .iter()
                .filter(|p| p.tag() == "CERTIFICATE")
                .map(|p| p.contents().to_vec().into())
                .collect();
            let key_der = keys
                .iter()
                .filter(|p| p.tag() == "PRIVATE KEY")
                .next()
                .expect("No private key found")
                .contents()
                .to_vec();
            let key = rustls::pki_types::PrivateKeyDer::try_from(key_der)
                .expect("Failed to create private key der");
            let config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .expect("Failed to create TLS config");
            tracing::info!("TLS enabled");
            Some(config)
        }
        _ => {
            tracing::info!("TLS disabled (no --tls-cert/--tls-key provided)");
            None
        }
    };

    tracing::info!(
        "SoliKV starting with {} shards, RESP on port {}, REST on port {}",
        num_shards,
        args.port,
        args.rest_port
    );
    if password.is_some() {
        tracing::info!("Authentication enabled");
    } else {
        tracing::info!("Authentication disabled (no password set)");
    }

    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());

    // Initialize cluster if enabled
    let cluster_manager: Option<Arc<ClusterManager>> = if args.cluster_enabled {
        let bind_ip = if args.bind == "0.0.0.0" || args.bind == "127.0.0.1" {
            "127.0.0.1".to_string()
        } else {
            args.bind.clone()
        };

        // Try to load existing node_id from cluster.json for stability
        let node_id = {
            let cluster_path = args.dir.join("cluster.json");
            if cluster_path.exists() {
                if let Ok(content) = std::fs::read_to_string(&cluster_path) {
                    if let Ok(state) =
                        serde_json::from_str::<solikv_cluster::ClusterStateSnapshot>(&content)
                    {
                        if state.ip == bind_ip && state.port == args.port {
                            tracing::info!("Using persisted node_id: {}", state.node_id);
                            state.node_id
                        } else {
                            generate_stable_node_id(&bind_ip, args.port)
                        }
                    } else {
                        generate_stable_node_id(&bind_ip, args.port)
                    }
                } else {
                    generate_stable_node_id(&bind_ip, args.port)
                }
            } else {
                generate_stable_node_id(&bind_ip, args.port)
            }
        };

        let gossip = GossipState::new(node_id.clone(), bind_ip.clone(), args.port);
        let cluster = Arc::new(ClusterManager::new(
            node_id,
            bind_ip.clone(),
            args.port,
            gossip,
        ));
        cluster.enable();
        tracing::info!("Cluster mode enabled");
        Some(cluster)
    } else {
        None
    };

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

    // Create shards with notification context (pubsub + flags) for expiry notifications
    let shards = Arc::new(solikv_engine::ShardManager::with_notifications(
        num_shards,
        pubsub.clone(),
        notify_flags.clone(),
    ));

    // --- Import Redis RDB if requested ---
    if let Some(ref redis_rdb_path) = args.import_redis_rdb {
        tracing::info!("Importing Redis RDB from {:?}...", redis_rdb_path);
        match solikv_persist::redis_rdb::import_redis_rdb(redis_rdb_path, num_shards) {
            Ok((shard_data, stats)) => {
                let mut total = 0usize;
                for (idx, entries) in shard_data.into_iter().enumerate() {
                    let count = entries.len();
                    if count > 0 {
                        shards.shard(idx).execute(|store| {
                            for (key, entry) in entries {
                                store.insert_entry(key, entry);
                            }
                            solikv_core::CommandResponse::ok()
                        });
                    }
                    total += count;
                }
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

    // --- Load RDB snapshots ---
    match solikv_persist::load_all_shards(&args.dir, &args.dbfilename, num_shards, |idx, f| {
        shards.shard(idx).execute(|store| {
            if let Err(e) = f(store) {
                tracing::error!("Failed to load RDB for shard {}: {}", idx, e);
            }
            solikv_core::CommandResponse::ok()
        });
        Ok(())
    }) {
        Ok(n) if n > 0 => {
            tracing::info!("Loaded RDB snapshots for {} shards from {:?}", n, args.dir)
        }
        Ok(_) => tracing::info!("No RDB snapshots found in {:?}, starting fresh", args.dir),
        Err(e) => tracing::warn!("RDB load error: {}", e),
    }

    // --- Load cluster state from RDB ---
    if let Some(ref cluster) = cluster_manager {
        let cluster_path = args.dir.join("cluster.json");
        if cluster_path.exists() {
            match std::fs::read_to_string(&cluster_path) {
                Ok(content) => {
                    match serde_json::from_str::<solikv_cluster::ClusterStateSnapshot>(&content) {
                        Ok(state) => {
                            cluster.import_state(&state);
                            tracing::info!("Loaded cluster state from {:?}", cluster_path);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse cluster state: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to read cluster state file: {}", e);
                }
            }
        }
    }

    // --- Replay AOF ---
    if args.appendonly {
        let aof_path = args.dir.join("appendonly.aof");
        if aof_path.exists() {
            match AofPersistence::replay(&aof_path) {
                Ok(commands) => {
                    let count = commands.len();
                    let replay_engine =
                        solikv_engine::CommandEngine::new(shards.clone(), pubsub.clone());
                    for cmd_args in commands {
                        if cmd_args.is_empty() {
                            continue;
                        }
                        let name = std::str::from_utf8(&cmd_args[0])
                            .unwrap_or("")
                            .to_uppercase();
                        replay_engine.execute(&name, &cmd_args[1..]);
                    }
                    tracing::info!("Replayed {} AOF commands from {:?}", count, aof_path);
                }
                Err(e) => tracing::warn!("AOF replay error: {}", e),
            }
        }
    }

    // --- Create lock-free AOF writer (channel-based, background task handles I/O + fsync) ---
    let aof_writer = if args.appendonly {
        std::fs::create_dir_all(&args.dir).ok();
        let aof_path = args.dir.join("appendonly.aof");
        match solikv_persist::spawn_aof_writer(&aof_path, fsync_policy) {
            Ok(writer) => {
                tracing::info!("AOF enabled ({:?} fsync) at {:?}", fsync_policy, aof_path);
                Some(writer)
            }
            Err(e) => {
                tracing::error!("Failed to open AOF file: {}", e);
                None
            }
        }
    } else {
        None
    };

    // --- Create engine with AOF ---
    let mut engine = solikv_engine::CommandEngine::new(shards.clone(), pubsub.clone())
        .with_notify_flags(notify_flags);
    if let Some(writer) = aof_writer {
        engine = engine.with_aof(writer);
    }
    let engine = Arc::new(engine);
    engine.init_self_ref(Arc::downgrade(&engine));

    // --- Spawn background RDB save task ---
    {
        let shards_rdb = shards.clone();
        let dir = args.dir.clone();
        let dbfilename = args.dbfilename.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                let ns = shards_rdb.num_shards();
                let shards_ref = &shards_rdb;
                if let Err(e) = solikv_persist::save_all_shards(&dir, &dbfilename, ns, |idx, f| {
                    shards_ref.shard(idx).with_store(|store| f(store))
                }) {
                    tracing::error!("Background RDB save error: {}", e);
                } else {
                    tracing::debug!("Background RDB snapshot saved");
                }
            }
        });
    }

    let resp_addr = format!("{}:{}", args.bind, args.port);
    let rest_addr = format!("{}:{}", args.bind, args.rest_port);

    tracing::info!("Starting RESP server on {}", resp_addr);
    tracing::info!("Starting REST server on {}", rest_addr);

    let engine_resp = engine.clone();
    let pubsub_resp = pubsub.clone();
    let password_resp = password.clone();
    let cluster_resp = cluster_manager.clone();
    let tls_config_resp = tls_config.clone();
    let resp_handle = tokio::spawn(async move {
        if let Err(e) = resp_server::run(
            &resp_addr,
            engine_resp,
            pubsub_resp,
            password_resp,
            cluster_resp,
            tls_config_resp,
        )
        .await
        {
            tracing::error!("RESP server error: {}", e);
        }
    });

    let engine_rest = engine.clone();
    let password_rest = password.clone();
    let rest_handle = tokio::spawn(async move {
        if let Err(e) = rest_server::run(&rest_addr, engine_rest, password_rest).await {
            tracing::error!("REST server error: {}", e);
        }
    });

    tokio::select! {
        _ = resp_handle => {},
        _ = rest_handle => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down — saving final RDB snapshot...");
            let ns = shards.num_shards();
            if let Err(e) = solikv_persist::save_all_shards(&args.dir, &args.dbfilename, ns, |idx, f| {
                shards.shard(idx).with_store(|store| f(store))
            }) {
                tracing::error!("Shutdown RDB save error: {}", e);
            } else {
                tracing::info!("Final RDB snapshot saved to {:?}", args.dir);
            }

            // Save cluster state
            if let Some(ref cluster) = cluster_manager {
                let cluster_state = cluster.export_state();
                let cluster_path = args.dir.join("cluster.json");
                match serde_json::to_string_pretty(&cluster_state) {
                    Ok(json) => {
                        if let Err(e) = std::fs::write(&cluster_path, json) {
                            tracing::error!("Failed to save cluster state: {}", e);
                        } else {
                            tracing::info!("Cluster state saved to {:?}", cluster_path);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to serialize cluster state: {}", e);
                    }
                }
            }
        }
    }
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
