//! Command semantics through the real RESP server and a real client, on a
//! multi-shard engine: multi-key commands across shards, transactions and
//! WATCH, TTL rules, the ZRANGE family, logical databases, and the
//! connection commands that used to answer OK without doing anything.

use redis::aio::MultiplexedConnection;
use redis::{cmd, RedisResult, Value};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

const SHARDS: usize = 8;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(21000);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start(password: Option<&str>) -> u16 {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(SHARDS));
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
    engine.set_tcp_port(port);
    let password = password.map(|p| Arc::new(p.to_string()));
    let addr = format!("127.0.0.1:{}", port);
    tokio::spawn(async move {
        solikv_server::resp_server::run_with_limits(
            &addr,
            engine,
            pubsub,
            password,
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

/// Two keys that live on different shards (and one sharing a tag).
fn keys_on_different_shards() -> (String, String) {
    let m = solikv_engine::ShardManager::new(SHARDS);
    let a = "key:a".to_string();
    let ia = m.shard_index_for_key(a.as_bytes());
    for i in 0.. {
        let b = format!("key:b{}", i);
        if m.shard_index_for_key(b.as_bytes()) != ia {
            return (a, b);
        }
    }
    unreachable!()
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

fn err_text<T: std::fmt::Debug>(r: RedisResult<T>) -> String {
    match r {
        Ok(v) => panic!("expected an error, got {:?}", v),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn rename_moves_a_key_across_shards_with_its_ttl() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let (a, b) = keys_on_different_shards();
    let _: () = q(&mut c, &["SET", &a, "v", "EX", "100"]).await.unwrap();
    let _: () = q(&mut c, &["RENAME", &a, &b]).await.unwrap();
    let got: Option<String> = q(&mut c, &["GET", &b]).await.unwrap();
    assert_eq!(got.as_deref(), Some("v"));
    let gone: i64 = q(&mut c, &["EXISTS", &a]).await.unwrap();
    assert_eq!(gone, 0);
    let ttl: i64 = q(&mut c, &["TTL", &b]).await.unwrap();
    assert!(ttl > 90 && ttl <= 100, "ttl {ttl}");

    // RENAMENX onto an existing key does nothing.
    let _: () = q(&mut c, &["SET", &a, "other"]).await.unwrap();
    let n: i64 = q(&mut c, &["RENAMENX", &a, &b]).await.unwrap();
    assert_eq!(n, 0);
    let n: i64 = q(&mut c, &["RENAMENX", &b, "fresh"]).await.unwrap();
    assert_eq!(n, 1);
    assert!(err_text(q::<()>(&mut c, &["RENAME", "missing", &b]).await).contains("no such key"));
}

#[tokio::test]
async fn set_and_sorted_set_combinations_see_every_shard() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let (a, b) = keys_on_different_shards();
    let _: i64 = q(&mut c, &["SADD", &a, "x", "y", "z"]).await.unwrap();
    let _: i64 = q(&mut c, &["SADD", &b, "y", "z", "w"]).await.unwrap();
    let mut inter: Vec<String> = q(&mut c, &["SINTER", &a, &b]).await.unwrap();
    inter.sort();
    assert_eq!(inter, vec!["y", "z"]);
    let n: i64 = q(&mut c, &["SUNIONSTORE", "dest", &a, &b]).await.unwrap();
    assert_eq!(n, 4);
    let n: i64 = q(&mut c, &["SDIFFSTORE", "dest2", &a, &b]).await.unwrap();
    assert_eq!(n, 1);
    let n: i64 = q(&mut c, &["SMOVE", &a, &b, "x"]).await.unwrap();
    assert_eq!(n, 1);
    let is: i64 = q(&mut c, &["SISMEMBER", &b, "x"]).await.unwrap();
    assert_eq!(is, 1);

    let _: i64 = q(&mut c, &["ZADD", "z:a", "1", "m1", "2", "m2"])
        .await
        .unwrap();
    let _: i64 = q(&mut c, &["ZADD", "z:b", "10", "m2", "20", "m3"])
        .await
        .unwrap();
    let n: i64 = q(&mut c, &["ZUNIONSTORE", "z:u", "2", "z:a", "z:b"])
        .await
        .unwrap();
    assert_eq!(n, 3);
    let s: String = q(&mut c, &["ZSCORE", "z:u", "m2"]).await.unwrap();
    assert_eq!(s, "12");
    let n: i64 = q(
        &mut c,
        &["ZINTERSTORE", "z:i", "2", "z:a", "z:b", "WEIGHTS", "1", "2"],
    )
    .await
    .unwrap();
    assert_eq!(n, 1);
    let s: String = q(&mut c, &["ZSCORE", "z:i", "m2"]).await.unwrap();
    assert_eq!(s, "22");
}

#[tokio::test]
async fn exec_is_atomic_for_other_clients() {
    let port = start(None).await;
    let (a, b) = keys_on_different_shards();
    let mut writer = conn(port).await;
    let _: () = q(&mut writer, &["MSET", &a, "0", &b, "0"]).await.unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let (a, b, stop) = (a.clone(), b.clone(), stop.clone());
        tokio::spawn(async move {
            let mut r = conn(port).await;
            let mut checks = 0;
            while !stop.load(Ordering::Relaxed) {
                let v: Vec<i64> = q(&mut r, &["MGET", &a, &b]).await.unwrap();
                assert_eq!(v[0], v[1], "saw half a transaction");
                checks += 1;
            }
            checks
        })
    };
    for _ in 0..300 {
        let mut p = redis::pipe();
        p.atomic().cmd("INCR").arg(&a).cmd("INCR").arg(&b);
        let _: (i64, i64) = p.query_async(&mut writer).await.unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    assert!(reader.await.unwrap() > 0);
}

#[tokio::test]
async fn watch_aborts_exec_when_the_key_changed() {
    let port = start(None).await;
    let mut c1 = conn(port).await;
    let mut c2 = conn(port).await;

    // Untouched: EXEC runs.
    let _: () = q(&mut c1, &["WATCH", "w"]).await.unwrap();
    let _: () = q(&mut c1, &["MULTI"]).await.unwrap();
    let _: Value = q(&mut c1, &["SET", "w", "mine"]).await.unwrap();
    let r: Value = q(&mut c1, &["EXEC"]).await.unwrap();
    assert!(matches!(r, Value::Array(ref v) if v.len() == 1), "{r:?}");

    // Changed by another client in between: EXEC returns nil.
    let _: () = q(&mut c1, &["WATCH", "w"]).await.unwrap();
    let _: () = q(&mut c2, &["SET", "w", "theirs"]).await.unwrap();
    let _: () = q(&mut c1, &["MULTI"]).await.unwrap();
    let _: Value = q(&mut c1, &["SET", "w", "mine"]).await.unwrap();
    let r: Value = q(&mut c1, &["EXEC"]).await.unwrap();
    assert_eq!(r, Value::Nil);
    let v: String = q(&mut c1, &["GET", "w"]).await.unwrap();
    assert_eq!(v, "theirs");

    // Expiry counts as a change.
    let _: () = q(&mut c1, &["SET", "e", "1", "PX", "50"]).await.unwrap();
    let _: () = q(&mut c1, &["WATCH", "e"]).await.unwrap();
    sleep(Duration::from_millis(120)).await;
    let _: Option<String> = q(&mut c2, &["GET", "e"]).await.unwrap();
    let _: () = q(&mut c1, &["MULTI"]).await.unwrap();
    let _: Value = q(&mut c1, &["SET", "e", "2"]).await.unwrap();
    let r: Value = q(&mut c1, &["EXEC"]).await.unwrap();
    assert_eq!(r, Value::Nil);

    // WATCH inside MULTI is an error; UNWATCH forgets.
    let _: () = q(&mut c1, &["WATCH", "u"]).await.unwrap();
    let _: () = q(&mut c1, &["UNWATCH"]).await.unwrap();
    let _: () = q(&mut c2, &["SET", "u", "x"]).await.unwrap();
    let _: () = q(&mut c1, &["MULTI"]).await.unwrap();
    assert!(err_text(q::<()>(&mut c1, &["WATCH", "u"]).await).contains("inside MULTI"));
    let r: Value = q(&mut c1, &["EXEC"]).await.unwrap();
    assert!(matches!(r, Value::Array(_)), "{r:?}");
}

#[tokio::test]
async fn a_bad_queued_command_aborts_the_transaction() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let _: () = q(&mut c, &["MULTI"]).await.unwrap();
    assert!(err_text(q::<()>(&mut c, &["NOSUCHCOMMAND"]).await).contains("unknown command"));
    let _: Value = q(&mut c, &["SET", "t", "1"]).await.unwrap();
    assert!(err_text(q::<()>(&mut c, &["GET"]).await).contains("wrong number"));
    assert!(err_text(q::<()>(&mut c, &["MULTI"]).await).contains("nested"));
    // redis-rs names the EXECABORT code "ExecAbortError".
    let e = err_text(q::<Value>(&mut c, &["EXEC"]).await);
    assert!(e.contains("discarded because of previous errors"), "{e}");
    let v: Option<String> = q(&mut c, &["GET", "t"]).await.unwrap();
    assert_eq!(v, None);
    assert!(err_text(q::<()>(&mut c, &["EXEC"]).await).contains("without MULTI"));
}

#[tokio::test]
async fn ttls_survive_modification_and_expire_takes_options() {
    let port = start(None).await;
    let mut c = conn(port).await;
    for (setup, modify) in [
        (vec!["SET", "n", "1", "EX", "100"], vec!["INCR", "n"]),
        (vec!["SET", "s", "a", "EX", "100"], vec!["APPEND", "s", "b"]),
        (
            vec!["SET", "r", "abc", "EX", "100"],
            vec!["SETRANGE", "r", "1", "x"],
        ),
        (
            vec!["SET", "bm", "a", "EX", "100"],
            vec!["SETBIT", "bm", "3", "1"],
        ),
        (
            vec!["SET", "f", "1.5", "EX", "100"],
            vec!["INCRBYFLOAT", "f", "1"],
        ),
    ] {
        let _: Value = q(&mut c, &setup).await.unwrap();
        let _: Value = q(&mut c, &modify).await.unwrap();
        let ttl: i64 = q(&mut c, &["TTL", setup[1]]).await.unwrap();
        assert!(ttl > 90, "{:?} dropped the TTL ({ttl})", modify);
    }
    // KEEPTTL keeps it; a plain SET drops it.
    let _: () = q(&mut c, &["SET", "n", "5", "KEEPTTL"]).await.unwrap();
    let ttl: i64 = q(&mut c, &["TTL", "n"]).await.unwrap();
    assert!(ttl > 90);
    assert!(err_text(q::<()>(&mut c, &["SET", "n", "5", "NX", "XX"]).await).contains("syntax"));
    assert!(
        err_text(q::<()>(&mut c, &["SET", "n", "5", "EX", "0"]).await).contains("invalid expire")
    );
    assert!(
        err_text(q::<()>(&mut c, &["SET", "n", "5", "EX", "5", "KEEPTTL"]).await)
            .contains("syntax")
    );
    let old: String = q(&mut c, &["SET", "n", "6", "GET"]).await.unwrap();
    assert_eq!(old, "5");

    let _: () = q(&mut c, &["SET", "k", "v"]).await.unwrap();
    let r: i64 = q(&mut c, &["EXPIRE", "k", "100", "XX"]).await.unwrap();
    assert_eq!(r, 0, "XX on a key without TTL");
    let r: i64 = q(&mut c, &["EXPIRE", "k", "100", "NX"]).await.unwrap();
    assert_eq!(r, 1);
    let r: i64 = q(&mut c, &["EXPIRE", "k", "200", "NX"]).await.unwrap();
    assert_eq!(r, 0);
    let r: i64 = q(&mut c, &["EXPIRE", "k", "50", "GT"]).await.unwrap();
    assert_eq!(r, 0);
    let r: i64 = q(&mut c, &["EXPIRE", "k", "50", "LT"]).await.unwrap();
    assert_eq!(r, 1);
    assert!(
        err_text(q::<()>(&mut c, &["EXPIRE", "k", "5", "NX", "XX"]).await)
            .contains("not compatible")
    );
    // A deadline in the past deletes.
    let r: i64 = q(&mut c, &["EXPIRE", "k", "-1"]).await.unwrap();
    assert_eq!(r, 1);
    let e: i64 = q(&mut c, &["EXISTS", "k"]).await.unwrap();
    assert_eq!(e, 0);
    let _: () = q(&mut c, &["SET", "k", "v", "PXAT", "99999999999999"])
        .await
        .unwrap();
    let t: i64 = q(&mut c, &["PEXPIRETIME", "k"]).await.unwrap();
    assert_eq!(t, 99999999999999);
    let v: String = q(&mut c, &["GETEX", "k", "PERSIST"]).await.unwrap();
    assert_eq!(v, "v");
    let t: i64 = q(&mut c, &["EXPIRETIME", "k"]).await.unwrap();
    assert_eq!(t, -1);
}

#[tokio::test]
async fn zrange_family_follows_redis() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let _: i64 = q(
        &mut c,
        &[
            "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await
    .unwrap();
    let v: Vec<String> = q(
        &mut c,
        &["ZRANGE", "z", "2", "5", "BYSCORE", "LIMIT", "1", "2"],
    )
    .await
    .unwrap();
    assert_eq!(v, vec!["c", "d"]);
    let v: Vec<String> = q(&mut c, &["ZRANGE", "z", "(4", "-inf", "BYSCORE", "REV"])
        .await
        .unwrap();
    assert_eq!(v, vec!["c", "b", "a"]);
    let v: Vec<String> = q(&mut c, &["ZRANGEBYSCORE", "z", "(1", "(3"])
        .await
        .unwrap();
    assert_eq!(v, vec!["b"]);
    let v: Vec<String> = q(
        &mut c,
        &["ZREVRANGEBYSCORE", "z", "+inf", "4", "WITHSCORES"],
    )
    .await
    .unwrap();
    assert_eq!(v, vec!["e", "5", "d", "4"]);
    let n: i64 = q(&mut c, &["ZCOUNT", "z", "(1", "3"]).await.unwrap();
    assert_eq!(n, 2);
    let v: Vec<String> = q(&mut c, &["ZRANGE", "z", "0", "1", "REV"]).await.unwrap();
    assert_eq!(v, vec!["e", "d"]);
    assert!(
        err_text(q::<()>(&mut c, &["ZRANGEBYSCORE", "z", "x", "3"]).await).contains("not a float")
    );
    assert!(
        err_text(q::<()>(&mut c, &["ZRANGE", "z", "0", "1", "LIMIT", "0", "1"]).await)
            .contains("LIMIT")
    );
    let n: i64 = q(&mut c, &["ZREMRANGEBYSCORE", "z", "-inf", "(2"])
        .await
        .unwrap();
    assert_eq!(n, 1);
    let n: i64 = q(&mut c, &["ZREMRANGEBYRANK", "z", "-1", "-1"])
        .await
        .unwrap();
    assert_eq!(n, 1);
    let n: i64 = q(&mut c, &["ZCARD", "z"]).await.unwrap();
    assert_eq!(n, 3);

    // Lexicographic ranges on equal scores.
    let _: i64 = q(
        &mut c,
        &["ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d"],
    )
    .await
    .unwrap();
    let v: Vec<String> = q(&mut c, &["ZRANGE", "lex", "[b", "(d", "BYLEX"])
        .await
        .unwrap();
    assert_eq!(v, vec!["b", "c"]);
    let n: i64 = q(&mut c, &["ZLEXCOUNT", "lex", "-", "+"]).await.unwrap();
    assert_eq!(n, 4);

    // ZADD options.
    let s: String = q(&mut c, &["ZADD", "z", "INCR", "10", "b"]).await.unwrap();
    assert_eq!(s, "12");
    let r: Value = q(&mut c, &["ZADD", "z", "XX", "INCR", "1", "nope"])
        .await
        .unwrap();
    assert_eq!(r, Value::Nil);
    assert!(
        err_text(q::<()>(&mut c, &["ZADD", "z", "abc", "m"]).await).contains("not a valid float")
    );
    assert!(
        err_text(q::<()>(&mut c, &["ZADD", "z", "NX", "XX", "1", "m"]).await)
            .contains("not compatible")
    );
    assert!(
        err_text(q::<()>(&mut c, &["ZADD", "z", "INCR", "1", "a", "2", "b"]).await)
            .contains("single")
    );
    let n: i64 = q(&mut c, &["ZADD", "none", "XX", "1", "m"]).await.unwrap();
    assert_eq!(n, 0);
    let e: i64 = q(&mut c, &["EXISTS", "none"]).await.unwrap();
    assert_eq!(e, 0, "ZADD XX on a missing key created it");
}

#[tokio::test]
async fn databases_are_separate() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let _: () = q(&mut c, &["SET", "k", "zero"]).await.unwrap();
    let _: () = q(&mut c, &["SELECT", "3"]).await.unwrap();
    let v: Option<String> = q(&mut c, &["GET", "k"]).await.unwrap();
    assert_eq!(v, None);
    let _: () = q(&mut c, &["SET", "k", "three"]).await.unwrap();
    let _: () = q(&mut c, &["SET", "only3", "x"]).await.unwrap();
    let n: i64 = q(&mut c, &["DBSIZE"]).await.unwrap();
    assert_eq!(n, 2);
    // FLUSHDB empties this database only.
    let _: () = q(&mut c, &["SELECT", "0"]).await.unwrap();
    let _: () = q(&mut c, &["FLUSHDB"]).await.unwrap();
    let _: () = q(&mut c, &["SELECT", "3"]).await.unwrap();
    let v: String = q(&mut c, &["GET", "k"]).await.unwrap();
    assert_eq!(v, "three");
    // MOVE and SWAPDB.
    let n: i64 = q(&mut c, &["MOVE", "only3", "5"]).await.unwrap();
    assert_eq!(n, 1);
    let _: () = q(&mut c, &["SWAPDB", "3", "5"]).await.unwrap();
    let v: String = q(&mut c, &["GET", "only3"]).await.unwrap();
    assert_eq!(v, "x");
    let info: String = q(&mut c, &["INFO", "keyspace"]).await.unwrap();
    assert!(
        info.contains("db3:keys=1") && info.contains("db5:keys=1"),
        "{info}"
    );
    assert!(err_text(q::<()>(&mut c, &["SELECT", "16"]).await).contains("out of range"));
    // Another connection starts in database 0.
    let mut c2 = conn(port).await;
    let n: i64 = q(&mut c2, &["DBSIZE"]).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn client_commands_are_real() {
    let port = start(None).await;
    let mut c1 = conn(port).await;
    let mut c2 = conn(port).await;
    let id1: i64 = q(&mut c1, &["CLIENT", "ID"]).await.unwrap();
    let id2: i64 = q(&mut c2, &["CLIENT", "ID"]).await.unwrap();
    assert_ne!(id1, id2);
    let _: () = q(&mut c2, &["CLIENT", "SETNAME", "worker-2"])
        .await
        .unwrap();
    let name: String = q(&mut c2, &["CLIENT", "GETNAME"]).await.unwrap();
    assert_eq!(name, "worker-2");
    let list: String = q(&mut c1, &["CLIENT", "LIST"]).await.unwrap();
    assert!(list.contains("name=worker-2"), "{list}");
    // Client-side caching is implemented: tracking turns on.
    let ok: String = q(&mut c1, &["CLIENT", "TRACKING", "ON"]).await.unwrap();
    assert_eq!(ok, "OK");
    let killed: i64 = q(&mut c1, &["CLIENT", "KILL", "ID", &id2.to_string()])
        .await
        .unwrap();
    assert_eq!(killed, 1);
    sleep(Duration::from_millis(100)).await;
    assert!(
        q::<String>(&mut c2, &["PING"]).await.is_err(),
        "killed client still served"
    );
}

#[tokio::test]
async fn server_introspection_tells_the_truth() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let info: String = q(&mut c, &["INFO", "server"]).await.unwrap();
    assert!(info.contains("redis_version:"), "{info}");
    assert!(info.contains(&format!("tcp_port:{}", port)), "{info}");
    assert!(!info.contains("# Keyspace"), "section filter ignored");
    let n: i64 = q(&mut c, &["COMMAND", "COUNT"]).await.unwrap();
    assert!(n > 100);
    let get: Value = q(&mut c, &["COMMAND", "INFO", "get"]).await.unwrap();
    assert!(
        matches!(get, Value::Array(ref v) if matches!(&v[0], Value::Array(i) if i[1] == Value::Int(2))),
        "{get:?}"
    );
    assert!(
        err_text(q::<()>(&mut c, &["CONFIG", "SET", "no-such-param", "1"]).await)
            .contains("Unknown option")
    );
    let cfg: Vec<String> = q(&mut c, &["CONFIG", "GET", "data*"]).await.unwrap();
    assert_eq!(cfg, vec!["databases", "16"]);
    assert!(
        err_text(q::<()>(&mut c, &["REPLICAOF", "10.0.0.1", "6379"]).await)
            .contains("not supported")
    );
    let _: () = q(&mut c, &["REPLICAOF", "NO", "ONE"]).await.unwrap();
    let subs: Vec<Value> = q(&mut c, &["PUBSUB", "NUMSUB", "nobody"]).await.unwrap();
    assert_eq!(subs.len(), 2);
    assert_eq!(subs[1], Value::Int(0));
    // SCAN TYPE and cursor validation.
    let _: () = q(&mut c, &["SET", "str", "v"]).await.unwrap();
    let _: i64 = q(&mut c, &["RPUSH", "lst", "v"]).await.unwrap();
    let (cur, keys): (String, Vec<String>) = {
        let mut cursor = "0".to_string();
        let mut all = Vec::new();
        loop {
            let (next, batch): (String, Vec<String>) =
                q(&mut c, &["SCAN", &cursor, "TYPE", "list", "COUNT", "100"])
                    .await
                    .unwrap();
            all.extend(batch);
            if next == "0" {
                break (next, all);
            }
            cursor = next;
        }
    };
    assert_eq!(cur, "0");
    assert_eq!(keys, vec!["lst"]);
    assert!(err_text(q::<()>(&mut c, &["SCAN", "abc"]).await).contains("invalid cursor"));
}

#[tokio::test]
async fn auth_takes_a_username_and_hello_negotiates_resp2() {
    let port = start(Some("pw")).await;
    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn roundtrip(s: &mut tokio::net::TcpStream, req: &[u8]) -> String {
        s.write_all(req).await.unwrap();
        sleep(Duration::from_millis(50)).await;
        let mut buf = vec![0u8; 4096];
        let n = s.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    }
    let r = roundtrip(&mut raw, b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$2\r\npw\r\n").await;
    assert!(r.starts_with("-WRONGPASS"), "{r}");
    // RESP3 is spoken, but HELLO without AUTH needs an authenticated client.
    let r = roundtrip(&mut raw, b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n").await;
    assert!(r.starts_with("-NOAUTH"), "{r}");
    let r = roundtrip(&mut raw, b"*2\r\n$5\r\nHELLO\r\n$1\r\n4\r\n").await;
    assert!(r.starts_with("-NOPROTO"), "{r}");
    let r = roundtrip(
        &mut raw,
        b"*5\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$2\r\npw\r\n",
    )
    .await;
    assert!(r.contains("proto") && r.contains("7.2.0"), "{r}");
    let r = roundtrip(&mut raw, b"*1\r\n$4\r\nPING\r\n").await;
    assert_eq!(r, "+PONG\r\n");

    // redis://default:pw@ URLs authenticate with two-argument AUTH.
    let mut c = redis::Client::open(format!("redis://default:pw@127.0.0.1:{}/", port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let pong: String = q(&mut c, &["PING"]).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn scripts_are_atomic_and_see_their_database() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let _: () = q(&mut c, &["SELECT", "2"]).await.unwrap();
    let _: () = q(&mut c, &["SET", "k", "in-2"]).await.unwrap();
    let v: String = q(
        &mut c,
        &["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"],
    )
    .await
    .unwrap();
    assert_eq!(v, "in-2");
    // SELECT inside a script switches only the script's later calls.
    let v: Option<String> = q(
        &mut c,
        &[
            "EVAL",
            "redis.call('SELECT', 0); return redis.call('GET', 'k')",
            "0",
        ],
    )
    .await
    .unwrap();
    assert_eq!(v, None);
    let v: String = q(&mut c, &["GET", "k"]).await.unwrap();
    assert_eq!(v, "in-2");
}

#[tokio::test]
async fn stream_noack_skips_the_pending_list() {
    let port = start(None).await;
    let mut c = conn(port).await;
    let _: String = q(&mut c, &["XADD", "st", "*", "f", "v"]).await.unwrap();
    let _: () = q(&mut c, &["XGROUP", "CREATE", "st", "g", "0"])
        .await
        .unwrap();
    let _: Value = q(
        &mut c,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "NOACK",
            "STREAMS",
            "st",
            ">",
        ],
    )
    .await
    .unwrap();
    let pending: Vec<Value> = q(&mut c, &["XPENDING", "st", "g"]).await.unwrap();
    assert_eq!(pending[0], Value::Int(0), "{pending:?}");
}
