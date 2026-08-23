use bytes::Bytes;
use std::sync::Arc;

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

/// Mirrors `--solo`: multi_thread runtime, one worker. All store access happens
/// inside spawned tasks, so everything runs on that single worker thread -- unless
/// `block_in_place` hands the worker's queue to a replacement OS thread, which is
/// what this test is here to catch.
#[test]
fn solo_store_borrows_never_overlap() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let pubsub = Arc::new(solikv_pubsub::PubSubBroker::new());
        let shards = Arc::new(solikv_engine::ShardManager::solo_plain());
        assert!(shards.is_solo());
        let engine = Arc::new(solikv_engine::CommandEngine::new(shards, pubsub));

        {
            let e = engine.clone();
            tokio::spawn(async move {
                for i in 0..15_000 {
                    e.execute("SET", &[b(&format!("k:{i}")), b("v")]);
                }
            })
            .await
            .unwrap();
        }

        let mut tasks = Vec::new();
        // KEYS and SAVE/BGSAVE go through `maybe_block_in_place`.
        for _ in 0..4 {
            let e = engine.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..120 {
                    e.execute("KEYS", &[b("*")]);
                    tokio::task::yield_now().await;
                }
            }));
        }
        // Mutating traffic: a `&mut ShardStore` overlapping the shared borrow above
        // is the actual undefined behaviour.
        for t in 0..8 {
            let e = engine.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..4_000usize {
                    e.execute("SET", &[b(&format!("k:{}", (i + t) % 15_000)), b("v2")]);
                    e.execute("GET", &[b(&format!("k:{}", (i + t) % 15_000))]);
                    if i % 16 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    });
}
