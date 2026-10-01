//! Hash field expiry (`HEXPIRE` and family, Redis 7.4) through the real
//! server: replies and errors, fields vanishing when due, expiry carried by
//! `RENAME`, and a restart from the append-only log keeping deadlines as
//! they were and fields that expired gone.

use redis::aio::MultiplexedConnection;
use redis::{cmd, RedisResult};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

const SHARDS: usize = 4;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(24500);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn serve(
    engine: Arc<solikv_engine::CommandEngine>,
    pubsub: Arc<solikv_pubsub::PubSubBroker>,
) -> u16 {
    let port = next_port();
    engine.set_tcp_port(port);
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

async fn start() -> u16 {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(SHARDS));
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
    serve(engine, pubsub).await
}

async fn conn(port: u16) -> MultiplexedConnection {
    redis::Client::open(format!("redis://127.0.0.1:{}/", port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn q<T: redis::FromRedisValue>(
    c: &mut MultiplexedConnection,
    args: &[&str],
) -> RedisResult<T> {
    let mut command = cmd(args[0]);
    for a in &args[1..] {
        command.arg(*a);
    }
    command.query_async(c).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hexpire_family_replies_like_redis() {
    let port = start().await;
    let mut c = conn(port).await;
    let _: i64 = q(&mut c, &["HSET", "h", "a", "1", "b", "2", "c", "3"])
        .await
        .unwrap();
    let r: Vec<i64> = q(&mut c, &["HEXPIRE", "h", "100", "FIELDS", "2", "a", "zz"])
        .await
        .unwrap();
    assert_eq!(r, vec![1, -2]);
    let r: Vec<i64> = q(
        &mut c,
        &["HEXPIRE", "h", "100", "NX", "FIELDS", "2", "a", "b"],
    )
    .await
    .unwrap();
    assert_eq!(r, vec![0, 1]);
    let r: Vec<i64> = q(&mut c, &["HTTL", "h", "FIELDS", "3", "a", "c", "zz"])
        .await
        .unwrap();
    assert!(r[0] > 90 && r[0] <= 100, "{r:?}");
    assert_eq!(&r[1..], &[-1, -2]);
    let r: Vec<i64> = q(&mut c, &["HPERSIST", "h", "FIELDS", "2", "a", "c"])
        .await
        .unwrap();
    assert_eq!(r, vec![1, -1]);
    // A past deadline deletes the field.
    let r: Vec<i64> = q(&mut c, &["HPEXPIREAT", "h", "1", "FIELDS", "1", "c"])
        .await
        .unwrap();
    assert_eq!(r, vec![2]);
    let n: i64 = q(&mut c, &["HLEN", "h"]).await.unwrap();
    assert_eq!(n, 2);
    // Argument errors.
    let e = q::<Vec<i64>>(&mut c, &["HEXPIRE", "h", "10", "FIELDS", "2", "a"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("numfields"), "{e}");
    let e = q::<Vec<i64>>(&mut c, &["HEXPIRE", "h", "10", "FIELDZ", "1", "a"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("FIELDS"), "{e}");
    let e = q::<Vec<i64>>(&mut c, &["HTTL", "h", "FIELDS", "0"])
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("greater than 0") || e.to_string().contains("arguments"),
        "{e}"
    );
    let _: () = q(&mut c, &["SET", "s", "x"]).await.unwrap();
    let e = q::<Vec<i64>>(&mut c, &["HEXPIRE", "s", "10", "FIELDS", "1", "a"])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("WRONGTYPE"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn due_fields_disappear_and_rename_keeps_deadlines() {
    let port = start().await;
    let mut c = conn(port).await;
    let _: i64 = q(&mut c, &["HSET", "h", "a", "1", "b", "2"]).await.unwrap();
    let _: Vec<i64> = q(&mut c, &["HPEXPIRE", "h", "80", "FIELDS", "1", "a"])
        .await
        .unwrap();
    let _: Vec<i64> = q(&mut c, &["HPEXPIRE", "h", "60000", "FIELDS", "1", "b"])
        .await
        .unwrap();
    let _: () = q(&mut c, &["RENAME", "h", "{x}moved"]).await.unwrap();
    let r: Vec<i64> = q(&mut c, &["HTTL", "{x}moved", "FIELDS", "1", "b"])
        .await
        .unwrap();
    assert!(r[0] > 50, "{r:?}");
    sleep(Duration::from_millis(150)).await;
    let all: Vec<String> = q(&mut c, &["HGETALL", "{x}moved"]).await.unwrap();
    assert_eq!(all, vec!["b".to_string(), "2".to_string()]);
    // Last field due: the key goes.
    let _: Vec<i64> = q(&mut c, &["HPEXPIRE", "{x}moved", "50", "FIELDS", "1", "b"])
        .await
        .unwrap();
    sleep(Duration::from_millis(120)).await;
    // Fields expire when the hash is next touched (or by the active cycle,
    // which this in-process server does not run): then the key is gone.
    let n: i64 = q(&mut c, &["HLEN", "{x}moved"]).await.unwrap();
    assert_eq!(n, 0);
    let n: i64 = q(&mut c, &["EXISTS", "{x}moved"]).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadlines_and_expiries_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = solikv_engine::persistence::PersistConfig {
        dir: dir.path().to_path_buf(),
        appendonly: true,
        auto_rewrite_percentage: 0,
        save: Vec::new(),
        ..Default::default()
    };
    let engine_for = |cfg: &solikv_engine::persistence::PersistConfig| {
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let shards = Arc::new(solikv_engine::ShardManager::new(SHARDS));
        let engine = Arc::new(
            solikv_engine::CommandEngine::new(shards, pubsub.clone()).with_persistence(Arc::new(
                solikv_engine::persistence::Persistence::new(cfg.clone()),
            )),
        );
        engine.init_self_ref(Arc::downgrade(&engine));
        engine.load_persisted().expect("load");
        (engine, pubsub)
    };
    let deadline;
    {
        let (engine, pubsub) = engine_for(&cfg);
        let port = serve(engine.clone(), pubsub).await;
        let mut c = conn(port).await;
        let _: i64 = q(
            &mut c,
            &["HSET", "h", "keep", "1", "short", "2", "plain", "3"],
        )
        .await
        .unwrap();
        let _: Vec<i64> = q(&mut c, &["HEXPIRE", "h", "3600", "FIELDS", "1", "keep"])
            .await
            .unwrap();
        let _: Vec<i64> = q(&mut c, &["HPEXPIRE", "h", "50", "FIELDS", "1", "short"])
            .await
            .unwrap();
        let r: Vec<i64> = q(&mut c, &["HPEXPIRETIME", "h", "FIELDS", "1", "keep"])
            .await
            .unwrap();
        deadline = r[0];
        sleep(Duration::from_millis(120)).await;
        // Read it: the due field is expired now, and logged as such.
        let n: i64 = q(&mut c, &["HLEN", "h"]).await.unwrap();
        assert_eq!(n, 2);
        engine.shutdown(Some(false)).unwrap();
    }
    // Restart later: the relative TTL was logged as its deadline.
    sleep(Duration::from_millis(1100)).await;
    let (engine, _) = engine_for(&cfg);
    let r = engine.execute(
        "HPEXPIRETIME",
        &["h", "FIELDS", "3", "keep", "short", "plain"].map(bytes::Bytes::from),
    );
    match r {
        solikv_core::CommandResponse::Array(items) => {
            let got: Vec<i64> = items
                .into_iter()
                .map(|i| match i {
                    solikv_core::CommandResponse::Integer(n) => n,
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(got, vec![deadline, -2, -1]);
        }
        other => panic!("{other:?}"),
    }
}

/// Redis 8's HGETDEL, HGETEX and HSETEX: replies, conditions and expiries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hgetdel_hgetex_hsetex_reply_like_redis() {
    let port = start().await;
    let mut c = conn(port).await;
    let _: i64 = q(&mut c, &["HSET", "h", "a", "1", "b", "2", "c", "3"])
        .await
        .unwrap();

    // HGETDEL: values, nil for an absent field; the key goes once empty.
    let r: Vec<Option<String>> = q(&mut c, &["HGETDEL", "h", "FIELDS", "2", "a", "zz"])
        .await
        .unwrap();
    assert_eq!(r, vec![Some("1".into()), None]);
    let n: i64 = q(&mut c, &["HLEN", "h"]).await.unwrap();
    assert_eq!(n, 2);
    let _: Vec<Option<String>> = q(&mut c, &["HGETDEL", "h", "FIELDS", "2", "b", "c"])
        .await
        .unwrap();
    let n: i64 = q(&mut c, &["EXISTS", "h"]).await.unwrap();
    assert_eq!(n, 0);
    let e = q::<Vec<Option<String>>>(&mut c, &["HGETDEL", "h", "FIELDS", "2", "a"]).await;
    assert!(e.is_err());

    // HGETEX: values, then the expiry changed.
    let _: i64 = q(&mut c, &["HSET", "g", "a", "1", "b", "2"]).await.unwrap();
    let r: Vec<Option<String>> = q(&mut c, &["HGETEX", "g", "EX", "100", "FIELDS", "1", "a"])
        .await
        .unwrap();
    assert_eq!(r, vec![Some("1".into())]);
    let t: Vec<i64> = q(&mut c, &["HTTL", "g", "FIELDS", "2", "a", "b"])
        .await
        .unwrap();
    assert!(t[0] > 90 && t[0] <= 100 && t[1] == -1, "{t:?}");
    let _: Vec<Option<String>> = q(&mut c, &["HGETEX", "g", "PERSIST", "FIELDS", "1", "a"])
        .await
        .unwrap();
    let t: Vec<i64> = q(&mut c, &["HTTL", "g", "FIELDS", "1", "a"]).await.unwrap();
    assert_eq!(t, vec![-1]);
    // A deadline already past: returned, then gone.
    let r: Vec<Option<String>> = q(&mut c, &["HGETEX", "g", "PXAT", "1", "FIELDS", "1", "b"])
        .await
        .unwrap();
    assert_eq!(r, vec![Some("2".into())]);
    let n: i64 = q(&mut c, &["HEXISTS", "g", "b"]).await.unwrap();
    assert_eq!(n, 0);
    assert!(
        q::<Vec<Option<String>>>(&mut c, &["HGETEX", "g", "EX", "0", "FIELDS", "1", "a"])
            .await
            .is_err()
    );

    // HSETEX: FNX / FXX all or nothing; KEEPTTL; no option drops expiries.
    let r: i64 = q(
        &mut c,
        &[
            "HSETEX", "s", "FNX", "EX", "100", "FIELDS", "2", "x", "1", "y", "2",
        ],
    )
    .await
    .unwrap();
    assert_eq!(r, 1);
    let r: i64 = q(
        &mut c,
        &["HSETEX", "s", "FNX", "FIELDS", "2", "y", "9", "z", "9"],
    )
    .await
    .unwrap();
    assert_eq!(r, 0, "y exists: nothing set");
    let r: i64 = q(
        &mut c,
        &["HSETEX", "s", "FXX", "FIELDS", "2", "y", "9", "z", "9"],
    )
    .await
    .unwrap();
    assert_eq!(r, 0, "z does not: nothing set");
    let v: Option<String> = q(&mut c, &["HGET", "s", "z"]).await.unwrap();
    assert_eq!(v, None);
    let r: i64 = q(
        &mut c,
        &["HSETEX", "s", "FXX", "KEEPTTL", "FIELDS", "1", "x", "10"],
    )
    .await
    .unwrap();
    assert_eq!(r, 1);
    let t: Vec<i64> = q(&mut c, &["HTTL", "s", "FIELDS", "2", "x", "y"])
        .await
        .unwrap();
    assert!(t[0] > 90 && t[1] > 90, "KEEPTTL kept x's: {t:?}");
    let _: i64 = q(&mut c, &["HSETEX", "s", "FIELDS", "1", "y", "3"])
        .await
        .unwrap();
    let t: Vec<i64> = q(&mut c, &["HTTL", "s", "FIELDS", "1", "y"]).await.unwrap();
    assert_eq!(t, vec![-1], "no option: expiry dropped, as HSET");
    let v: Option<String> = q(&mut c, &["HGET", "s", "x"]).await.unwrap();
    assert_eq!(v.as_deref(), Some("10"));
    assert!(q::<i64>(&mut c, &["HSETEX", "s", "FIELDS", "2", "x", "1"])
        .await
        .is_err());
}

/// What HGETDEL/HGETEX/HSETEX did comes back from the log: values, the
/// fields they removed, and the deadlines they set or kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hgetdel_hgetex_hsetex_replay_from_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = solikv_engine::persistence::PersistConfig {
        dir: dir.path().to_path_buf(),
        appendonly: true,
        auto_rewrite_percentage: 0,
        save: Vec::new(),
        ..Default::default()
    };
    let engine_for = |cfg: &solikv_engine::persistence::PersistConfig| {
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let shards = Arc::new(solikv_engine::ShardManager::new(SHARDS));
        let engine = Arc::new(
            solikv_engine::CommandEngine::new(shards, pubsub.clone()).with_persistence(Arc::new(
                solikv_engine::persistence::Persistence::new(cfg.clone()),
            )),
        );
        engine.init_self_ref(Arc::downgrade(&engine));
        engine.load_persisted().expect("load");
        (engine, pubsub)
    };
    let run = |e: &solikv_engine::CommandEngine, args: &[&str]| {
        e.execute(
            args[0],
            &args[1..]
                .iter()
                .map(|a| bytes::Bytes::from(a.to_string()))
                .collect::<Vec<_>>(),
        )
    };
    let (x_at, kept_at);
    {
        let (engine, _) = engine_for(&cfg);
        run(&engine, &["HSET", "h", "a", "1", "b", "2", "c", "3"]);
        run(&engine, &["HGETDEL", "h", "FIELDS", "1", "a"]);
        run(
            &engine,
            &["HGETEX", "h", "PX", "500000", "FIELDS", "1", "b"],
        );
        run(&engine, &["HGETEX", "h", "PXAT", "1", "FIELDS", "1", "c"]);
        run(
            &engine,
            &[
                "HSETEX", "k", "EX", "3600", "FIELDS", "2", "x", "1", "y", "2",
            ],
        );
        run(
            &engine,
            &["HSETEX", "k", "KEEPTTL", "FIELDS", "1", "x", "9"],
        );
        run(&engine, &["HSETEX", "k", "FIELDS", "1", "y", "8"]);
        kept_at = match run(&engine, &["HPEXPIRETIME", "k", "FIELDS", "1", "x"]) {
            solikv_core::CommandResponse::Array(v) => match &v[0] {
                solikv_core::CommandResponse::Integer(n) => *n,
                o => panic!("{o:?}"),
            },
            o => panic!("{o:?}"),
        };
        x_at = match run(&engine, &["HPEXPIRETIME", "h", "FIELDS", "1", "b"]) {
            solikv_core::CommandResponse::Array(v) => format!("{:?}", v[0]),
            o => panic!("{o:?}"),
        };
        engine.shutdown(Some(false)).unwrap();
    }
    let (engine, _) = engine_for(&cfg);
    let all = |key: &str| format!("{:?}", run(&engine, &["HGETALL", key]));
    let h = all("h");
    assert!(
        h.contains("\"b\"") && !h.contains("\"a\"") && !h.contains("\"c\""),
        "{h}"
    );
    let k = all("k");
    assert!(k.contains("\"9\"") && k.contains("\"8\""), "{k}");
    let at = |key: &str, f: &str| match run(&engine, &["HPEXPIRETIME", key, "FIELDS", "1", f]) {
        solikv_core::CommandResponse::Array(v) => format!("{:?}", v[0]),
        o => panic!("{o:?}"),
    };
    assert_eq!(at("h", "b"), x_at, "HGETEX's deadline, as it was");
    assert_eq!(
        at("k", "x"),
        format!("{:?}", solikv_core::CommandResponse::Integer(kept_at))
    );
    assert_eq!(
        at("k", "y"),
        format!("{:?}", solikv_core::CommandResponse::Integer(-1))
    );
}
