//! Snapshots that do not stall the clients of the shard being saved.
//!
//! `SAVE` in multi-shard mode and `BGSAVE` in solo mode used to serialize a
//! whole shard in one go, holding it (solo: holding the only worker) for
//! the duration. Both now take the stepped point-in-time snapshot. Each
//! test puts every key on one shard and times another client's commands
//! against that shard while the save runs: with a whole-shard capture, one
//! of them waits for most of the save.

use redis::aio::MultiplexedConnection;
use redis::cmd;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use solikv_engine::persistence::{PersistConfig, Persistence};
use solikv_server::resp_server::ConnLimits;

/// Keys on the one shard every test uses (`{t}` hashes them together).
const KEYS: usize = 1_000_000;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(24600);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn config(dir: &std::path::Path) -> PersistConfig {
    PersistConfig {
        dir: dir.to_path_buf(),
        appendonly: false,
        auto_rewrite_percentage: 0,
        save: Vec::new(),
        ..Default::default()
    }
}

fn engine(
    shards: solikv_engine::ShardManager,
    cfg: &PersistConfig,
    pubsub: Arc<solikv_pubsub::PubSubBroker>,
) -> Arc<solikv_engine::CommandEngine> {
    let engine = Arc::new(
        solikv_engine::CommandEngine::new(Arc::new(shards), pubsub)
            .with_persistence(Arc::new(Persistence::new(cfg.clone()))),
    );
    engine.init_self_ref(Arc::downgrade(&engine));
    engine
}

/// Serve `engine` (already loaded) on a fresh port.
async fn serve(
    engine: Arc<solikv_engine::CommandEngine>,
    pubsub: Arc<solikv_pubsub::PubSubBroker>,
) -> u16 {
    let port = next_port();
    let addr = format!("127.0.0.1:{}", port);
    tokio::spawn(async move {
        solikv_server::resp_server::run_with_limits(
            &addr,
            engine,
            pubsub,
            None,
            None,
            None,
            ConnLimits::default(),
        )
        .await
        .unwrap();
    });
    sleep(Duration::from_millis(100)).await;
    port
}

async fn conn(port: u16) -> MultiplexedConnection {
    redis::Client::open(format!("redis://127.0.0.1:{}/", port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn fill(c: &mut MultiplexedConnection) {
    let value = "v".repeat(32);
    for batch in 0..KEYS / 1000 {
        let mut m = cmd("MSET");
        for i in batch * 1000..(batch + 1) * 1000 {
            m.arg(format!("{{t}}k:{}", i)).arg(&value);
        }
        let _: () = m.query_async(c).await.unwrap();
    }
}

/// Time `op` against the shard over and over until `stop`; the slowest.
fn probe<F, Fut>(stop: Arc<AtomicBool>, mut op: F) -> tokio::task::JoinHandle<(Duration, usize)>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let mut worst = Duration::ZERO;
        let mut n = 0;
        while !stop.load(Ordering::Relaxed) {
            let t = Instant::now();
            op().await;
            worst = worst.max(t.elapsed());
            n += 1;
        }
        (worst, n)
    })
}

/// `SAVE` (multi-shard): the caller waits for the whole save, the other
/// clients of the shard being saved do not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn save_lets_the_other_clients_of_the_shard_in() {
    // A watchdog: a hang must fail the test, not the whole run.
    tokio::time::timeout(Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let e = engine(solikv_engine::ShardManager::new(8), &cfg, pubsub.clone());
        let port = serve(e, pubsub).await;
        let mut c = conn(port).await;
        fill(&mut c).await;

        let stop = Arc::new(AtomicBool::new(false));
        let pc = conn(port).await;
        let probe = probe(stop.clone(), move || {
            let mut pc = pc.clone();
            async move {
                let _: i64 = cmd("INCR").arg("{t}n").query_async(&mut pc).await.unwrap();
            }
        });
        sleep(Duration::from_millis(50)).await;
        let t0 = Instant::now();
        let _: () = cmd("SAVE").query_async(&mut c).await.unwrap();
        let save = t0.elapsed();
        stop.store(true, Ordering::Relaxed);
        let (worst, n) = probe.await.unwrap();

        assert!(n > 0);
        assert!(
            save > Duration::from_millis(50),
            "the save took {save:?}: too quick to tell a stall from a step"
        );
        assert!(
            worst * 4 < save,
            "a client of the shard waited {worst:?} during a {save:?} SAVE"
        );
        assert!(dir.path().join("dump.rdb").exists());
    })
    .await
    .expect("test timed out");
}

/// `BGSAVE` in solo mode: the store belongs to the runtime's only worker,
/// so the snapshot is stepped there between the connections' commands.
/// The reply comes at once, clients are answered throughout, and the file
/// holds the dataset as of the command — not a write made right after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn solo_bgsave_keeps_answering_and_saves_the_instant() {
    tokio::time::timeout(Duration::from_secs(300), async {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let shards = solikv_engine::ShardManager::solo(pubsub.clone(), Arc::new(AtomicU16::new(0)));
        let e = engine(shards, &cfg, pubsub.clone());
        let port = serve(e, pubsub).await;
        let mut c = conn(port).await;
        fill(&mut c).await;
        let _: () = cmd("SET")
            .arg("{t}marker")
            .arg("before")
            .query_async(&mut c)
            .await
            .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let pc = conn(port).await;
        let probe = probe(stop.clone(), move || {
            let mut pc = pc.clone();
            async move {
                let _: Option<String> =
                    cmd("GET").arg("{t}k:1").query_async(&mut pc).await.unwrap();
            }
        });
        sleep(Duration::from_millis(50)).await;
        let t0 = Instant::now();
        let reply: String = cmd("BGSAVE").query_async(&mut c).await.unwrap();
        let replied = t0.elapsed();
        assert_eq!(reply, "Background saving started");
        // Written after the snapshot's instant: not in the file.
        let _: () = cmd("SET")
            .arg("{t}marker")
            .arg("after")
            .query_async(&mut c)
            .await
            .unwrap();
        loop {
            let info: String = cmd("INFO")
                .arg("persistence")
                .query_async(&mut c)
                .await
                .unwrap();
            if info.contains("rdb_bgsave_in_progress:0") {
                break;
            }
            sleep(Duration::from_millis(2)).await;
        }
        let total = t0.elapsed();
        stop.store(true, Ordering::Relaxed);
        let (worst, n) = probe.await.unwrap();

        assert!(n > 0);
        assert!(
            total > Duration::from_millis(50),
            "the save took {total:?}: too quick to tell a stall from a step"
        );
        assert!(
            replied * 4 < total,
            "BGSAVE answered after {replied:?} of a {total:?} save"
        );
        assert!(
            worst * 4 < total,
            "a client waited {worst:?} during a {total:?} BGSAVE"
        );

        // The file, loaded by a fresh (multi-shard) server.
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let check = engine(solikv_engine::ShardManager::new(4), &cfg, pubsub);
        check.load_persisted().expect("load the snapshot");
        let marker = check.execute("GET", &[bytes::Bytes::from("{t}marker")]);
        assert_eq!(
            format!("{marker:?}"),
            format!(
                "{:?}",
                solikv_core::CommandResponse::BulkString(bytes::Bytes::from("before"))
            )
        );
        let size = check.execute("DBSIZE", &[]);
        assert_eq!(
            format!("{size:?}"),
            format!(
                "{:?}",
                solikv_core::CommandResponse::Integer(KEYS as i64 + 1)
            )
        );
    })
    .await
    .expect("test timed out");
}
