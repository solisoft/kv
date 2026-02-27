use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use solikv_persist::{AofPersistence, FsyncPolicy};

mod resp_server;
mod rest_server;

#[derive(Parser, Debug)]
#[command(name = "solikv", about = "SoliKV - High-Performance In-Memory Database")]
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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&args.log_level)),
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

    tracing::info!(
        "SoliKV starting with {} shards, RESP on port {}, REST on port {}",
        num_shards, args.port, args.rest_port
    );

    let shards = Arc::new(solikv_engine::ShardManager::new(num_shards));
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());

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
        Ok(n) if n > 0 => tracing::info!("Loaded RDB snapshots for {} shards from {:?}", n, args.dir),
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
                    let replay_engine = solikv_engine::CommandEngine::new(shards.clone(), pubsub.clone());
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
    let mut engine = solikv_engine::CommandEngine::new(shards.clone(), pubsub.clone());
    if let Some(writer) = aof_writer {
        engine = engine.with_aof(writer);
    }
    let engine = Arc::new(engine);

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
    let resp_handle = tokio::spawn(async move {
        if let Err(e) = resp_server::run(&resp_addr, engine_resp, pubsub_resp).await {
            tracing::error!("RESP server error: {}", e);
        }
    });

    let engine_rest = engine.clone();
    let rest_handle = tokio::spawn(async move {
        if let Err(e) = rest_server::run(&rest_addr, engine_rest).await {
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
