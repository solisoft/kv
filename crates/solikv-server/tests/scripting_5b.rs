//! Scripting as Redis clients use it — scripts in the shape of BullMQ's,
//! Sidekiq's and a classic rate limiter, `FUNCTION`/`FCALL`, `-BUSY` and
//! `SCRIPT KILL` — and RESP3 on the wire.

use redis::aio::MultiplexedConnection;
use redis::{cmd, RedisResult, Value};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;

use solikv_server::resp_server::ConnLimits;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(23500);

fn next_port() -> u16 {
    loop {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

async fn start_with(configure: impl FnOnce(&solikv_engine::CommandEngine)) -> u16 {
    let port = next_port();
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(solikv_engine::ShardManager::new(4));
    let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub.clone()));
    engine.init_self_ref(Arc::downgrade(&engine));
    engine.set_tcp_port(port);
    configure(&engine);
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
    start_with(|_| {}).await
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

/// A BullMQ-shaped script: job data travels as msgpack and JSON, and the
/// script writes a hash, a sorted set and a list.
#[tokio::test]
async fn bullmq_style_script_with_cmsgpack_and_cjson() {
    let port = start().await;
    let mut c = conn(port).await;
    let script = r#"
        local opts = cmsgpack.unpack(ARGV[1])
        local data = cjson.decode(ARGV[2])
        local jobId = redis.call("INCR", KEYS[1] .. ":id")
        local jobKey = KEYS[1] .. ":" .. jobId
        redis.call("HSET", jobKey, "name", opts.name, "data", cjson.encode(data),
                   "timestamp", opts.timestamp, "delay", opts.delay or 0)
        if (opts.delay or 0) > 0 then
            redis.call("ZADD", KEYS[1] .. ":delayed", opts.timestamp + opts.delay, jobId)
        else
            redis.call("LPUSH", KEYS[1] .. ":wait", jobId)
        end
        return {jobId, cmsgpack.pack({ok = true, n = #data.items}), cjson.encode(data.items)}
    "#;
    // cmsgpack of {name="email", timestamp=1700000000000, delay=500}
    let packed: Vec<u8> = {
        let mut v = vec![0x83];
        v.extend_from_slice(&[
            0xa4, b'n', b'a', b'm', b'e', 0xa5, b'e', b'm', b'a', b'i', b'l',
        ]);
        v.extend_from_slice(&[0xa9]);
        v.extend_from_slice(b"timestamp");
        v.push(0xcf);
        v.extend_from_slice(&1_700_000_000_000u64.to_be_bytes());
        v.extend_from_slice(&[0xa5]);
        v.extend_from_slice(b"delay");
        v.extend_from_slice(&[0xcd, 0x01, 0xf4]);
        v
    };
    let r: Vec<Value> = cmd("EVAL")
        .arg(script)
        .arg(1)
        .arg("bull:q")
        .arg(packed)
        .arg(r#"{"items":[1,2,3],"to":"a@b.c"}"#)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(r[0], Value::Int(1));
    match &r[1] {
        // {n=3, ok=true} in either key order
        Value::BulkString(b) => assert_eq!(b.len(), 1 + 2 + 1 + 3 + 1),
        other => panic!("{other:?}"),
    }
    assert_eq!(r[2], Value::BulkString(b"[1,2,3]".to_vec()));
    let score: f64 = q(&mut c, &["ZSCORE", "bull:q:delayed", "1"]).await.unwrap();
    assert_eq!(score, 1_700_000_000_500.0);
    let data: String = q(&mut c, &["HGET", "bull:q:1", "data"]).await.unwrap();
    assert!(data.contains(r#""to":"a@b.c""#), "{data}");
}

/// Sidekiq 7's scheduler: the unified ZRANGE BYSCORE LIMIT form from Lua.
#[tokio::test]
async fn sidekiq_style_scheduler_script() {
    let port = start().await;
    let mut c = conn(port).await;
    for (score, job) in [(10, "a"), (20, "b"), (30, "c")] {
        let _: i64 = q(&mut c, &["ZADD", "schedule", &score.to_string(), job])
            .await
            .unwrap();
    }
    let script = r#"
        local key, now = KEYS[1], ARGV[1]
        local jobs = redis.call("zrange", key, "-inf", now, "byscore", "limit", 0, 1)
        if jobs[1] then
          redis.call("zrem", key, jobs[1])
          return jobs[1]
        end
    "#;
    let sha: String = q(&mut c, &["SCRIPT", "LOAD", script]).await.unwrap();
    let mut got = Vec::new();
    loop {
        let r: Option<String> = q(&mut c, &["EVALSHA", &sha, "1", "schedule", "25"])
            .await
            .unwrap();
        match r {
            Some(j) => got.push(j),
            None => break,
        }
    }
    assert_eq!(got, vec!["a", "b"]);
}

/// A token bucket rate limiter: numbers, math, HMGET/HSET and PEXPIRE.
#[tokio::test]
async fn token_bucket_rate_limiter_script() {
    let port = start().await;
    let mut c = conn(port).await;
    let script = r#"
        local key = KEYS[1]
        local rate, capacity, now, requested =
            tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
        local state = redis.call("HMGET", key, "tokens", "ts")
        local tokens = tonumber(state[1]) or capacity
        local ts = tonumber(state[2]) or now
        tokens = math.min(capacity, tokens + (math.max(0, now - ts) * rate / 1000))
        local allowed = tokens >= requested
        if allowed then tokens = tokens - requested end
        redis.call("HSET", key, "tokens", tokens, "ts", now)
        redis.call("PEXPIRE", key, math.ceil(capacity / rate * 1000))
        return { allowed and 1 or 0, math.floor(tokens) }
    "#;
    let mut allowed = 0;
    for _ in 0..7 {
        let r: Vec<i64> = cmd("EVAL")
            .arg(script)
            .arg(1)
            .arg("rl:user")
            .arg(1) // 1 token/s
            .arg(5) // capacity 5
            .arg(1_000_000)
            .arg(1)
            .query_async(&mut c)
            .await
            .unwrap();
        allowed += r[0];
    }
    assert_eq!(allowed, 5);
    let ttl: i64 = q(&mut c, &["PTTL", "rl:user"]).await.unwrap();
    assert!(ttl > 0 && ttl <= 5000, "{ttl}");
    // Fractional token counts reached HSET as the float, not truncated.
    let r: String = cmd("EVAL")
        .arg("redis.call('HSET', 'h', 'f', 2.5) return redis.call('HGET', 'h', 'f')")
        .arg(0)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(r, "2.5");
}

#[tokio::test]
async fn eval_ro_and_no_writes_scripts_cannot_write() {
    let port = start().await;
    let mut c = conn(port).await;
    let e = q::<Value>(
        &mut c,
        &["EVAL_RO", "return redis.call('SET', 'k', 'v')", "0"],
    )
    .await;
    assert!(e.unwrap_err().to_string().contains("read-only"));
    let e = q::<Value>(
        &mut c,
        &[
            "EVAL",
            "#!lua flags=no-writes\nreturn redis.call('SET', 'k', 'v')",
            "0",
        ],
    )
    .await;
    assert!(e.unwrap_err().to_string().contains("read-only"));
    let v: Option<String> = q(&mut c, &["EVAL_RO", "return redis.call('GET', 'k')", "0"])
        .await
        .unwrap();
    assert_eq!(v, None);
    let e = q::<Value>(&mut c, &["EVAL", "#!lua flags=nope\nreturn 1", "0"]).await;
    assert!(e.unwrap_err().to_string().contains("Unexpected flag"));
}

#[tokio::test]
async fn functions_load_call_list_and_delete() {
    let port = start().await;
    let mut c = conn(port).await;
    let code = "#!lua name=counters\n\
        redis.register_function('bump', function(keys, args) return redis.call('INCRBY', keys[1], args[1]) end)\n\
        redis.register_function{function_name='peek', callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}";
    let name: String = q(&mut c, &["FUNCTION", "LOAD", code]).await.unwrap();
    assert_eq!(name, "counters");
    let n: i64 = q(&mut c, &["FCALL", "bump", "1", "ctr", "5"])
        .await
        .unwrap();
    assert_eq!(n, 5);
    let v: String = q(&mut c, &["FCALL_RO", "peek", "1", "ctr"]).await.unwrap();
    assert_eq!(v, "5");
    let e = q::<Value>(&mut c, &["FCALL_RO", "bump", "1", "ctr", "1"]).await;
    assert!(e.unwrap_err().to_string().contains("write flag"));
    let list: Vec<Value> = q(&mut c, &["FUNCTION", "LIST"]).await.unwrap();
    assert_eq!(list.len(), 1);
    let e = q::<Value>(&mut c, &["FUNCTION", "LOAD", code]).await;
    assert!(e.unwrap_err().to_string().contains("already exists"));
    let dump: Vec<u8> = q(&mut c, &["FUNCTION", "DUMP"]).await.unwrap();
    let _: String = q(&mut c, &["FUNCTION", "DELETE", "counters"])
        .await
        .unwrap();
    let e = q::<Value>(&mut c, &["FCALL", "bump", "1", "ctr", "1"]).await;
    assert!(e.unwrap_err().to_string().contains("Function not found"));
    let _: String = cmd("FUNCTION")
        .arg("RESTORE")
        .arg(dump)
        .query_async(&mut c)
        .await
        .unwrap();
    let n: i64 = q(&mut c, &["FCALL", "bump", "1", "ctr", "1"])
        .await
        .unwrap();
    assert_eq!(n, 6);
}

/// A script past `busy-reply-threshold` makes other clients get `-BUSY`
/// (not hang), and `SCRIPT KILL` ends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_script_is_reported_and_killable() {
    // A script that is never killed holds a runtime worker forever, and the
    // runtime's shutdown would wait on it: fail the process instead.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(60));
        eprintln!("WATCHDOG: busy_script_is_reported_and_killable did not finish");
        std::process::exit(3);
    });
    let port = start_with(|e| e.set_busy_threshold_ms(200)).await;
    let mut runner = conn(port).await;
    let looping =
        tokio::spawn(
            async move { q::<Value>(&mut runner, &["EVAL", "while true do end", "0"]).await },
        );
    sleep(Duration::from_millis(400)).await;
    let mut other = conn(port).await;
    let e = tokio::time::timeout(
        Duration::from_secs(5),
        q::<String>(&mut other, &["GET", "x"]),
    )
    .await
    .expect("a command during a busy script must not hang");
    assert!(e.unwrap_err().to_string().contains("BUSY"));
    kill_when_running(&mut other).await;
    let r = tokio::time::timeout(Duration::from_secs(5), looping)
        .await
        .unwrap()
        .unwrap();
    assert!(r.unwrap_err().to_string().contains("killed"));
    let e = q::<Value>(&mut other, &["SCRIPT", "KILL"]).await;
    assert!(e
        .unwrap_err()
        .to_string()
        .contains("No scripts in execution"));
    // A script's own pcall cannot swallow the kill.
    let mut runner = conn(port).await;
    let looping = tokio::spawn(async move {
        q::<Value>(
            &mut runner,
            &[
                "EVAL",
                "while true do pcall(function() while true do end end) end",
                "0",
            ],
        )
        .await
    });
    sleep(Duration::from_millis(400)).await;
    kill_when_running(&mut other).await;
    let r = tokio::time::timeout(Duration::from_secs(5), looping)
        .await
        .unwrap()
        .unwrap();
    assert!(r.is_err());
}

/// `SCRIPT KILL`, retried until the script has started (NOTBUSY before).
async fn kill_when_running(c: &mut MultiplexedConnection) {
    for _ in 0..100 {
        match q::<String>(c, &["SCRIPT", "KILL"]).await {
            Ok(ok) => {
                assert_eq!(ok, "OK");
                return;
            }
            Err(e) if e.to_string().contains("No scripts in execution") => {
                sleep(Duration::from_millis(50)).await
            }
            Err(e) => panic!("SCRIPT KILL: {e}"),
        }
    }
    panic!("the script never started");
}

async fn raw(port: u16) -> tokio::net::TcpStream {
    tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap()
}

async fn send(s: &mut tokio::net::TcpStream, args: &[&str]) -> String {
    let mut out = format!("*{}\r\n", args.len());
    for a in args {
        out.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    s.write_all(out.as_bytes()).await.unwrap();
    sleep(Duration::from_millis(60)).await;
    let mut buf = vec![0u8; 8192];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[tokio::test]
async fn resp3_types_on_the_wire() {
    let port = start().await;
    let mut s = raw(port).await;
    let hello = send(&mut s, &["HELLO", "3"]).await;
    assert!(hello.starts_with("%7\r\n"), "{hello:?}");
    assert!(hello.contains("proto\r\n:3"), "{hello:?}");
    assert_eq!(send(&mut s, &["GET", "missing"]).await, "_\r\n");
    send(&mut s, &["HSET", "h", "f", "v"]).await;
    assert_eq!(
        send(&mut s, &["HGETALL", "h"]).await,
        "%1\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    send(&mut s, &["ZADD", "z", "1.5", "m"]).await;
    assert_eq!(send(&mut s, &["ZSCORE", "z", "m"]).await, ",1.5\r\n");
    assert_eq!(
        send(&mut s, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]).await,
        "*1\r\n*2\r\n$1\r\nm\r\n,1.5\r\n"
    );
    send(&mut s, &["SADD", "s", "a"]).await;
    assert_eq!(send(&mut s, &["SMEMBERS", "s"]).await, "~1\r\n$1\r\na\r\n");
    assert!(send(&mut s, &["INFO", "server"]).await.starts_with('='));
    // RESP2 clients still get the flat shapes.
    let mut s2 = raw(port).await;
    assert_eq!(
        send(&mut s2, &["HGETALL", "h"]).await,
        "*2\r\n$1\r\nf\r\n$1\r\nv\r\n"
    );
    assert_eq!(send(&mut s2, &["GET", "missing"]).await, "$-1\r\n");
    // Pub/sub messages are pushes, and a subscribed RESP3 client can still
    // run commands.
    let sub = send(&mut s, &["SUBSCRIBE", "ch"]).await;
    assert!(sub.starts_with(">3\r\n$9\r\nsubscribe"), "{sub:?}");
    let mut p = conn(port).await;
    let _: i64 = q(&mut p, &["PUBLISH", "ch", "hi"]).await.unwrap();
    sleep(Duration::from_millis(60)).await;
    let mut buf = vec![0u8; 1024];
    let n = s.read(&mut buf).await.unwrap();
    assert_eq!(
        &buf[..n],
        b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n"
    );
    assert_eq!(send(&mut s, &["GET", "missing"]).await, "_\r\n");
    let e = send(&mut s2, &["HELLO", "4"]).await;
    assert!(e.starts_with("-NOPROTO"), "{e:?}");
}

/// Inside `EXEC`, each reply has the RESP3 type its command's has on its
/// own (a map for HGETALL, a double for ZSCORE, null for a missing key);
/// RESP2 clients get the flat forms.
#[tokio::test]
async fn resp3_exec_replies_are_typed_per_command() {
    let port = start().await;
    let mut s = raw(port).await;
    send(&mut s, &["HELLO", "3"]).await;
    send(&mut s, &["HSET", "eh", "f", "v"]).await;
    send(&mut s, &["ZADD", "ez", "2.5", "m"]).await;
    assert_eq!(send(&mut s, &["MULTI"]).await, "+OK\r\n");
    for cmd in [
        &["HGETALL", "eh"][..],
        &["ZSCORE", "ez", "m"],
        &["GET", "missing"],
    ] {
        assert_eq!(send(&mut s, cmd).await, "+QUEUED\r\n");
    }
    assert_eq!(
        send(&mut s, &["EXEC"]).await,
        "*3\r\n%1\r\n$1\r\nf\r\n$1\r\nv\r\n,2.5\r\n_\r\n"
    );

    let mut s2 = raw(port).await;
    send(&mut s2, &["MULTI"]).await;
    send(&mut s2, &["HGETALL", "eh"]).await;
    send(&mut s2, &["ZSCORE", "ez", "m"]).await;
    assert_eq!(
        send(&mut s2, &["EXEC"]).await,
        "*2\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n$3\r\n2.5\r\n"
    );
}
