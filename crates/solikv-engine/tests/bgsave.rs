use bytes::Bytes;
use solikv_core::CommandResponse;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

fn engine(dir: &std::path::Path, solo: bool) -> Arc<solikv_engine::CommandEngine> {
    let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
    let shards = Arc::new(if solo {
        solikv_engine::ShardManager::solo_plain()
    } else {
        solikv_engine::ShardManager::new(4)
    });
    Arc::new(solikv_engine::CommandEngine::new(shards, pubsub).with_rdb(dir.to_path_buf(), "dump"))
}

fn rdb_files(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".rdb"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Waits for `rdb_bgsave_in_progress:0` rather than sleeping a fixed amount.
async fn await_bgsave(engine: &solikv_engine::CommandEngine) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while engine.bgsave_state().in_progress() {
        assert!(Instant::now() < deadline, "BGSAVE never completed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bgsave_replies_before_the_files_are_written() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), false);
    for i in 0..2_000 {
        engine.execute("SET", &[b(&format!("k:{i}")), b("v")]);
    }

    let resp = engine.execute("BGSAVE", &[]);
    match &resp {
        CommandResponse::SimpleString(s) => assert_eq!(&s[..], b"Background saving started"),
        other => panic!("expected a simple status, got {other:?}"),
    }
    assert!(engine.bgsave_state().in_progress());

    await_bgsave(&engine).await;
    assert_eq!(rdb_files(dir.path()).len(), 4, "one file per shard");
    assert!(engine.bgsave_state().last_status_ok());
    assert!(engine.bgsave_state().last_save_unix() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_bgsave_is_refused_while_one_runs() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), false);
    for i in 0..20_000 {
        engine.execute("SET", &[b(&format!("k:{i}")), b("v")]);
    }

    assert!(!engine.execute("BGSAVE", &[]).is_error());
    // Only meaningful while the first save is still running; if it already finished
    // the second call is legitimately accepted, so don't assert on a lost race.
    if engine.bgsave_state().in_progress() {
        let second = engine.execute("BGSAVE", &[]);
        match &second {
            CommandResponse::Error(e) => {
                assert!(e.contains("Background save already in progress"), "{e}")
            }
            other => panic!("expected an error, got {other:?}"),
        }
    }
    await_bgsave(&engine).await;

    // The guard must be released, so a later BGSAVE is accepted again.
    assert!(!engine.execute("BGSAVE", &[]).is_error());
    await_bgsave(&engine).await;
}

/// The reason BGSAVE serializes on the runtime and writes on a blocking thread:
/// solo mode's store is an unlocked `UnsafeCell`, so a blocking thread touching it
/// would alias `&mut ShardStore`. Debug builds assert on overlap, so this test
/// fails loudly if the split is ever collapsed.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn bgsave_is_safe_in_solo_mode_under_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), true);
    assert!(engine.shards.is_solo());
    for i in 0..10_000 {
        engine.execute("SET", &[b(&format!("k:{i}")), b("v")]);
    }

    assert!(!engine.execute("BGSAVE", &[]).is_error());

    let mut tasks = Vec::new();
    for t in 0..4 {
        let e = engine.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..3_000usize {
                e.execute("SET", &[b(&format!("k:{}", (i + t) % 10_000)), b("v2")]);
                e.execute("GET", &[b(&format!("k:{}", (i + t) % 10_000))]);
                if i % 16 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    await_bgsave(&engine).await;
    assert_eq!(rdb_files(dir.path()).len(), 1, "solo mode has one shard");
    assert!(engine.bgsave_state().last_status_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lastsave_tracks_completion_and_save_is_still_synchronous() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), false);
    engine.execute("SET", &[b("k"), b("v")]);

    match engine.execute("LASTSAVE", &[]) {
        CommandResponse::Integer(0) => {}
        other => panic!("expected 0 before any save, got {other:?}"),
    }

    // SAVE blocks: the files exist by the time it replies.
    assert!(!engine.execute("SAVE", &[]).is_error());
    assert_eq!(rdb_files(dir.path()).len(), 4);
    let after_save = match engine.execute("LASTSAVE", &[]) {
        CommandResponse::Integer(n) => n,
        other => panic!("expected an integer, got {other:?}"),
    };
    assert!(after_save > 0, "SAVE should update LASTSAVE");

    assert!(!engine.execute("BGSAVE", &[]).is_error());
    await_bgsave(&engine).await;
    match engine.execute("LASTSAVE", &[]) {
        CommandResponse::Integer(n) => assert!(n >= after_save),
        other => panic!("expected an integer, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn info_reports_bgsave_progress() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), false);
    engine.execute("SET", &[b("k"), b("v")]);

    assert!(!engine.execute("BGSAVE", &[]).is_error());
    await_bgsave(&engine).await;

    let info = match engine.execute("INFO", &[]) {
        CommandResponse::BulkString(b) => String::from_utf8(b.to_vec()).unwrap(),
        other => panic!("expected a bulk string, got {other:?}"),
    };
    assert!(info.contains("# Persistence"), "{info}");
    assert!(info.contains("rdb_bgsave_in_progress:0"), "{info}");
    assert!(info.contains("rdb_last_bgsave_status:ok"), "{info}");
    assert!(!info.contains("rdb_last_save_time:0\r\n"), "{info}");
}

/// Embedded / unit-test use has no Tokio runtime to spawn onto, so BGSAVE must
/// still save rather than silently doing nothing.
#[test]
fn bgsave_falls_back_to_a_blocking_save_without_a_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), false);
    engine.execute("SET", &[b("k"), b("v")]);

    assert!(!engine.execute("BGSAVE", &[]).is_error());
    assert_eq!(rdb_files(dir.path()).len(), 4);
    assert!(!engine.bgsave_state().in_progress());
    assert!(engine.bgsave_state().last_save_unix() > 0);
}
