use mimalloc::MiMalloc;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::Parser;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use nix::sys::signal::kill;
use nix::unistd::Pid;

use solikv_persist::FsyncPolicy;

use solikv_cluster::{generate_stable_node_id, ClusterManager, GossipState};
use solikv_server::cluster_dump;
use solikv_server::resp_server;
use solikv_server::rest_server;

#[derive(Parser, Debug)]
#[command(
    name = "solikv",
    version,
    about = "SoliKV - High-Performance In-Memory Database"
)]
struct Args {
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

    /// TLS certificate file. With --tls-key, enables TLS on both the RESP
    /// and the REST port. The cluster bus is not encrypted (it is
    /// authenticated; see SECURITY.md).
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<String>,

    /// TLS private key file
    #[arg(long, value_name = "PATH")]
    tls_key: Option<String>,

    /// Reserved for client-certificate (mTLS) verification. Not implemented:
    /// the server refuses to start with it rather than silently skip it.
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

fn main() {
    let args = Args::parse();

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
    for w in &early_warnings {
        tracing::warn!("{}", w);
    }

    if args.tls_client_ca.is_some() {
        eprintln!(
            "ERROR: --tls-client-ca (client certificate verification) is not implemented. \
             Refusing to start rather than accept clients without the check you asked for."
        );
        std::process::exit(1);
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
    let protected = args.protected_mode.to_lowercase() != "no";
    if !is_loopback && password.is_none() && protected {
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
                .find(|p| p.tag() == "PRIVATE KEY")
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

        // The bus secret. Required, and checked before anything is bound.
        //
        // This used to be the whole of cluster mode: build a `GossipState`, call
        // `enable()`, log success, and never start the server. Every node then
        // listened for no peer and reached none — three nodes were three
        // independent nodes, each reporting healthy. The server is started
        // below, and it authenticates every frame.
        //
        // Refusing without a secret rather than starting open is the same rule
        // `db/` now follows: gossip decides who is in the cluster and which node
        // owns which slots, so an unauthenticated bus is a stranger deciding the
        // topology.
        let bus_secret = match cluster_password.as_deref() {
            Some(pw) => match solikv_cluster::auth::ClusterSecret::new(pw) {
                Ok(secret) => secret,
                Err(e) => {
                    eprintln!("ERROR: cluster mode needs a usable bus secret: {e}");
                    eprintln!("       set --cluster-password-file or SOLIKV_CLUSTER_PASSWORD");
                    std::process::exit(1);
                }
            },
            None => {
                eprintln!(
                    "ERROR: --cluster-enabled requires a cluster password: the gossip bus decides \
                     cluster membership and slot ownership, and an unauthenticated bus lets \
                     anyone who can reach the port decide both."
                );
                eprintln!("       set --cluster-password-file or SOLIKV_CLUSTER_PASSWORD");
                std::process::exit(1);
            }
        };

        let gossip = GossipState::new(node_id.clone(), bind_ip.clone(), args.port);
        let (gossip_tx, mut gossip_rx) = tokio::sync::mpsc::unbounded_channel();

        // The bus listens on the data port plus the standard offset, on the same
        // address the node advertises — not on loopback, which the previous
        // version hardcoded and which cannot reach a peer on another machine.
        let bus_port = args.port + solikv_cluster::CLUSTER_BUS_PORT_OFFSET;
        let bus_bind: std::net::SocketAddr = format!("{}:{}", bind_ip, bus_port)
            .parse()
            .unwrap_or_else(|e| {
                eprintln!(
                    "ERROR: cannot form a cluster bus address from {bind_ip}:{bus_port}: {e}"
                );
                std::process::exit(1);
            });

        let bus_secret_for_tasks = bus_secret.clone();
        let gossip_for_tasks = gossip.clone();

        if let Err(e) = solikv_cluster::gossip::start_gossip_server(
            bus_bind,
            bus_secret,
            gossip.clone(),
            gossip_tx,
        )
        .await
        {
            eprintln!("ERROR: cluster bus failed to bind {bus_bind}: {e}");
            std::process::exit(1);
        }

        let cluster = Arc::new(ClusterManager::new(
            node_id,
            bind_ip.clone(),
            args.port,
            gossip,
        ));
        cluster.enable();

        // Drain the authenticated frames into the manager, and answer the ones
        // that need answering. Without this the channel fills and the server
        // blocks — the quiet version of the bug this whole change is about.
        let cluster_for_gossip = cluster.clone();
        let reply_secret = bus_secret_for_tasks.clone();
        tokio::spawn(async move {
            while let Some(message) = gossip_rx.recv().await {
                let sender = match &message {
                    solikv_cluster::GossipMessage::Ping {
                        node_id, ip, port, ..
                    } => format!("{}:{}", ip, port + solikv_cluster::CLUSTER_BUS_PORT_OFFSET)
                        .parse::<std::net::SocketAddr>()
                        .ok()
                        .map(|addr| (addr, node_id.clone())),
                    _ => None,
                };
                if let Some(reply) = cluster_for_gossip.handle_gossip(message) {
                    // A pong goes back over a fresh connection to the address the
                    // ping advertised, not back down the inbound socket: the
                    // inbound one may be a client that only writes, and the
                    // advertised address is the one the rest of the cluster uses.
                    if let Some((peer, peer_id)) = sender {
                        let secret = reply_secret.clone();
                        tokio::spawn(async move {
                            // Addressed to the pinger by id: a pong vouches for
                            // liveness, and an unaddressed one could be replayed
                            // at every node to keep a dead peer looking alive.
                            if let Err(e) = solikv_cluster::gossip::send_frame(
                                peer,
                                &reply,
                                &secret,
                                std::time::Duration::from_secs(3),
                                &peer_id,
                            )
                            .await
                            {
                                tracing::debug!(%peer, error = %e, "gossip pong failed");
                            }
                        });
                    }
                }
            }
        });

        // The outbound half. Nothing dialled a peer before this: there was a
        // listener and no caller, so even a correctly configured cluster sat
        // silent. One round every second, failures logged and skipped — a single
        // unreachable node is what the failure detector is for, not a reason to
        // stop pinging everyone after it.
        let gossip_for_round = gossip_for_tasks.clone();
        let round_secret = bus_secret_for_tasks.clone();
        let cluster_for_round = cluster.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                // Read fresh each round: an operator can assign or drop slots at
                // any moment, and a claim captured once would keep announcing
                // ranges this node has given up.
                let my_slots = cluster_for_round.my_slot_ranges();
                solikv_cluster::gossip::gossip_round_with_slots(
                    &gossip_for_round,
                    &round_secret,
                    solikv_cluster::CLUSTER_BUS_PORT_OFFSET,
                    std::time::Duration::from_secs(3),
                    &my_slots,
                )
                .await;
            }
        });

        tracing::info!(%bus_bind, "cluster mode enabled, gossip bus authenticated");
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
            save_interval: args.save_interval,
        },
    ));
    let engine = solikv_engine::CommandEngine::new(shards.clone(), pubsub.clone())
        .with_notify_flags(notify_flags)
        .with_lua_memory_limit(args.lua_memory_limit)
        .with_persistence(persistence.clone());
    let engine = Arc::new(engine);
    engine.init_self_ref(Arc::downgrade(&engine));

    // --- Import Redis RDB if requested ---
    if let Some(ref redis_rdb_path) = args.import_redis_rdb {
        tracing::info!("Importing Redis RDB from {:?}...", redis_rdb_path);
        match solikv_persist::redis_rdb::import_redis_rdb(redis_rdb_path, num_shards) {
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

    // --- Periodic snapshots and automatic log rewrites ---
    {
        let engine_bg = engine.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_save = std::time::Instant::now();
            loop {
                ticker.tick().await;
                let p = engine_bg.persistence().clone();
                if p.should_auto_rewrite() {
                    tracing::info!("Append-only log has grown; rewriting it");
                    if let Err(e) = p.bgrewrite(&engine_bg.shards) {
                        tracing::warn!("Automatic log rewrite not started: {}", e);
                    }
                }
                let every = p.config().save_interval;
                if every > 0 && last_save.elapsed() >= std::time::Duration::from_secs(every) {
                    last_save = std::time::Instant::now();
                    let dirty = engine_bg.dirty();
                    let saved_at = p
                        .stats
                        .rdb_dirty_at_last_save
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if dirty != saved_at {
                        if let Err(e) = p.bgsave(&engine_bg.shards, dirty) {
                            tracing::debug!("Periodic snapshot skipped: {}", e);
                        }
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

    // Save cluster state
    if let Some(ref cluster) = cluster_manager {
        let cluster_state = cluster.export_state();
        let cluster_path = args.dir.join("cluster.json");
        match serde_json::to_string_pretty(&cluster_state) {
            Ok(json) => {
                if let Err(e) = solikv_persist::fsutil::write_atomic(&cluster_path, json) {
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

    // The pidfile names this process only in daemon mode; leave anyone
    // else's alone.
    let pidfile = args.dir.join("solikv.pid");
    if fs::read_to_string(&pidfile).is_ok_and(|p| p.trim() == std::process::id().to_string()) {
        let _ = fs::remove_file(&pidfile);
    }
    std::process::exit(0);
}

/// `64mb`, `1g`, `4096` → bytes.
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
