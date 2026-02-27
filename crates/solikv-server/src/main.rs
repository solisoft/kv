use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use solikv_persist::{AofPersistence, FsyncPolicy};

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

    /// Keyspace notification flags (e.g. "KEA" for all events, "" to disable)
    #[arg(long, default_value = "")]
    notify_keyspace_events: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

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

    let password: Option<Arc<String>> = args.requirepass.map(Arc::new);

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

    let engine_resp = engine.clone();
    let pubsub_resp = pubsub.clone();
    let password_resp = password.clone();
    let resp_handle = tokio::spawn(async move {
        if let Err(e) = resp_server::run(&resp_addr, engine_resp, pubsub_resp, password_resp).await
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
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
