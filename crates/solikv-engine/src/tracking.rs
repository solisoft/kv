//! Client-side caching (`CLIENT TRACKING`).
//!
//! In the default mode the server remembers which keys each tracking client
//! read, and when one of them changes — a write, an expiry, an eviction —
//! tells that client once and forgets it (the client re-reads to cache it
//! again). In broadcasting mode (`BCAST`) a client registers prefixes and
//! hears about every change to a key under them, remembered or not.
//!
//! Invalidations go to the client itself as a RESP3 `invalidate` push, or,
//! with `REDIRECT <id>`, to another connection — for RESP2 clients, one
//! subscribed to `__redis__:invalidate`, which receives them as messages on
//! that channel.
//!
//! With no tracking client, writes pay one atomic load.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::session::ClientRegistry;

/// A tracking client's options.
#[derive(Debug, Clone, Default)]
pub struct TrackingOpts {
    pub redirect: Option<u64>,
    pub bcast: bool,
    pub prefixes: Vec<Bytes>,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
}

/// What a connection receives out of band.
#[derive(Debug, Clone)]
pub enum Push {
    /// Keys that changed; `None`: everything (a flush).
    Invalidate(Option<Vec<Bytes>>),
}

/// A remembered reader: who read, who to tell, and whether their own
/// writes count.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Reader {
    owner: u64,
    target: u64,
    noloop: bool,
}

struct Bcast {
    owner: u64,
    target: u64,
    prefixes: Vec<Bytes>,
    noloop: bool,
}

pub struct Tracking {
    clients: AtomicUsize,
    table: Mutex<HashMap<Bytes, Vec<Reader>>>,
    bcast: Mutex<Vec<Bcast>>,
    /// `tracking-table-max-keys` (0 = unbounded).
    pub max_keys: AtomicUsize,
    registry: Arc<ClientRegistry>,
}

impl Tracking {
    pub fn new(registry: Arc<ClientRegistry>) -> Self {
        Self {
            clients: AtomicUsize::new(0),
            table: Mutex::new(HashMap::new()),
            bcast: Mutex::new(Vec::new()),
            max_keys: AtomicUsize::new(1_000_000),
            registry,
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.clients.load(Ordering::Relaxed) > 0
    }

    pub fn tracking_clients(&self) -> usize {
        self.clients.load(Ordering::Relaxed)
    }

    pub fn tracked_keys(&self) -> usize {
        self.table.lock().len()
    }

    /// A client turned tracking on.
    pub fn enable(&self, owner: u64, opts: &TrackingOpts) {
        self.clients.fetch_add(1, Ordering::Relaxed);
        if opts.bcast {
            self.bcast.lock().push(Bcast {
                owner,
                target: opts.redirect.unwrap_or(owner),
                prefixes: opts.prefixes.clone(),
                noloop: opts.noloop,
            });
        }
    }

    /// A client turned tracking off (or disconnected).
    pub fn disable(&self, owner: u64) {
        self.clients.fetch_sub(1, Ordering::Relaxed);
        self.bcast.lock().retain(|b| b.owner != owner);
        // Default-mode entries are dropped lazily: an invalidation sent to a
        // client that is gone is simply not delivered.
    }

    /// `owner` read `keys` (default mode).
    pub fn remember(&self, owner: u64, target: u64, noloop: bool, keys: &[Bytes]) {
        if keys.is_empty() {
            return;
        }
        let r = Reader {
            owner,
            target,
            noloop,
        };
        let mut evicted = Vec::new();
        {
            let mut t = self.table.lock();
            for k in keys {
                let readers = t.entry(k.clone()).or_default();
                if !readers.contains(&r) {
                    readers.push(r);
                }
            }
            // Bounded: past the limit, forget (and invalidate) keys.
            let max = self.max_keys.load(Ordering::Relaxed);
            while max > 0 && t.len() > max {
                let Some(k) = t.keys().next().cloned() else {
                    break;
                };
                if let Some(readers) = t.remove(&k) {
                    evicted.push((k, readers));
                }
            }
        }
        for (k, readers) in evicted {
            for r in readers {
                self.send(r.target, Push::Invalidate(Some(vec![k.clone()])));
            }
        }
    }

    /// `keys` changed, written by client `writer` (0: the server itself —
    /// expiry, eviction).
    pub fn invalidate(&self, keys: &[Bytes], writer: u64) {
        if keys.is_empty() {
            return;
        }
        let mut out: HashMap<u64, Vec<Bytes>> = HashMap::new();
        {
            let mut t = self.table.lock();
            for k in keys {
                if let Some(readers) = t.remove(k) {
                    for r in readers {
                        if r.noloop && r.owner == writer {
                            continue;
                        }
                        out.entry(r.target).or_default().push(k.clone());
                    }
                }
            }
        }
        {
            let b = self.bcast.lock();
            for c in b.iter() {
                if c.noloop && c.owner == writer {
                    continue;
                }
                for k in keys {
                    if c.prefixes.is_empty() || c.prefixes.iter().any(|p| k.starts_with(p)) {
                        out.entry(c.target).or_default().push(k.clone());
                    }
                }
            }
        }
        for (target, keys) in out {
            self.send(target, Push::Invalidate(Some(keys)));
        }
    }

    /// Everything changed (`FLUSHDB`/`FLUSHALL`): every tracking client is
    /// told to drop its whole cache.
    pub fn flush(&self) {
        let mut targets: Vec<u64> = Vec::new();
        {
            let mut t = self.table.lock();
            for readers in t.values() {
                for r in readers {
                    if !targets.contains(&r.target) {
                        targets.push(r.target);
                    }
                }
            }
            t.clear();
        }
        for c in self.bcast.lock().iter() {
            if !targets.contains(&c.target) {
                targets.push(c.target);
            }
        }
        for t in targets {
            self.send(t, Push::Invalidate(None));
        }
    }

    fn send(&self, target: u64, push: Push) {
        self.registry.push(target, push);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mode_invalidates_once_and_honours_noloop() {
        let reg = Arc::new(ClientRegistry::default());
        let a = reg.register("a".into(), "l".into());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        a.set_push(tx);
        let t = Tracking::new(reg.clone());
        t.enable(a.id, &TrackingOpts::default());
        t.remember(a.id, a.id, false, &[Bytes::from("k")]);
        t.invalidate(&[Bytes::from("k")], 99);
        assert!(
            matches!(rx.try_recv(), Ok(Push::Invalidate(Some(k))) if k == vec![Bytes::from("k")])
        );
        // Forgotten after one invalidation.
        t.invalidate(&[Bytes::from("k")], 99);
        assert!(rx.try_recv().is_err());
        // NOLOOP: the client's own write is not reported.
        t.remember(a.id, a.id, true, &[Bytes::from("k")]);
        t.invalidate(&[Bytes::from("k")], a.id);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn bcast_prefixes_and_flush() {
        let reg = Arc::new(ClientRegistry::default());
        let a = reg.register("a".into(), "l".into());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        a.set_push(tx);
        let t = Tracking::new(reg.clone());
        t.enable(
            a.id,
            &TrackingOpts {
                bcast: true,
                prefixes: vec![Bytes::from("user:")],
                ..Default::default()
            },
        );
        t.invalidate(&[Bytes::from("user:1"), Bytes::from("other")], 5);
        assert!(
            matches!(rx.try_recv(), Ok(Push::Invalidate(Some(k))) if k == vec![Bytes::from("user:1")])
        );
        t.flush();
        assert!(matches!(rx.try_recv(), Ok(Push::Invalidate(None))));
        t.disable(a.id);
        assert!(!t.is_active());
    }
}
