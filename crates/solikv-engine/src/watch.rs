//! `WATCH`: which clients watch which keys, and marking them dirty.
//!
//! A write marks its keys **under the lock of the shard it runs on** (see
//! `ShardHandle::run`), and `EXEC` checks its flag while holding the locks of
//! every shard its watched keys and queued commands live on. So a write
//! either lands before the `EXEC` and aborts it, or waits for the `EXEC` to
//! finish — never in between. A multi-shard write marks all its keys at its
//! first shard, before any of them has changed; that can only abort an
//! `EXEC` that would otherwise have run, never let one run on stale data.
//!
//! Marking is also conservative about *what* changed: a write command marks
//! every key it names, even when it turned out to change nothing (`SADD` of a
//! member already there), and expiry marks the expired key. Redis does the
//! same for most commands; the cost is a retry, not a wrong answer.
//!
//! Nothing here runs unless some client watches something: the per-write
//! check is one relaxed load of a counter that is zero on a server whose
//! clients never `WATCH`.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// One client's "a watched key changed" flag.
#[derive(Debug, Default)]
pub struct WatchFlag {
    dirty: AtomicBool,
}

impl WatchFlag {
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct Map {
    /// `(db, key)` → flags of the clients watching it.
    keys: HashMap<(u32, Bytes), Vec<Arc<WatchFlag>>>,
}

/// Every watched key of the server.
#[derive(Default)]
pub struct WatchRegistry {
    /// Number of `(key, client)` registrations. Zero ⇒ nothing to mark.
    active: AtomicUsize,
    map: Mutex<Map>,
}

impl WatchRegistry {
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed) != 0
    }

    /// Start watching `(db, key)` for `flag`'s client.
    pub fn watch(&self, db: u32, key: Bytes, flag: &Arc<WatchFlag>) {
        let mut m = self.map.lock();
        let list = m.keys.entry((db, key)).or_default();
        if !list.iter().any(|f| Arc::ptr_eq(f, flag)) {
            list.push(flag.clone());
            self.active.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Stop watching these keys for `flag`'s client.
    pub fn unwatch(&self, keys: &[(u32, Bytes)], flag: &Arc<WatchFlag>) {
        if keys.is_empty() {
            return;
        }
        let mut m = self.map.lock();
        for k in keys {
            if let Some(list) = m.keys.get_mut(k) {
                let before = list.len();
                list.retain(|f| !Arc::ptr_eq(f, flag));
                let removed = before - list.len();
                if removed > 0 {
                    self.active.fetch_sub(removed, Ordering::Relaxed);
                }
                if list.is_empty() {
                    m.keys.remove(k);
                }
            }
        }
    }

    /// A write changed `key` in `db`.
    pub fn touch(&self, db: u32, key: &[u8]) {
        let m = self.map.lock();
        // `Bytes` hashes like `[u8]`; build the lookup key without copying
        // when possible is not offered by HashMap for tuples, so copy.
        if let Some(list) = m.keys.get(&(db, Bytes::copy_from_slice(key))) {
            for f in list {
                f.dirty.store(true, Ordering::Release);
            }
        }
    }

    /// Every key of `db` changed (`FLUSHDB`, `SWAPDB`), or of every database
    /// (`FLUSHALL`) when `db` is `None`.
    pub fn touch_db(&self, db: Option<u32>) {
        let m = self.map.lock();
        for ((d, _), list) in m.keys.iter() {
            if db.is_none_or(|db| db == *d) {
                for f in list {
                    f.dirty.store(true, Ordering::Release);
                }
            }
        }
    }
}

/// What the running write command changes, for [`WatchRegistry`] marking by
/// the shard it runs on. Set by the engine per command, only while some
/// client watches something.
#[derive(Debug, Clone)]
pub(crate) enum Touch {
    Keys(u32, Vec<Bytes>),
    Db(u32),
    All,
}

thread_local! {
    static TOUCH: std::cell::RefCell<Option<Touch>> = const { std::cell::RefCell::new(None) };
}

/// Sets the running command's [`Touch`] for its duration (commands nest in
/// scripts; the outer one is restored).
pub(crate) struct TouchGuard {
    prev: Option<Touch>,
}

impl TouchGuard {
    pub(crate) fn set(t: Option<Touch>) -> Self {
        Self {
            prev: TOUCH.with(|c| c.replace(t)),
        }
    }
}

impl Drop for TouchGuard {
    fn drop(&mut self) {
        let prev = self.prev.take();
        TOUCH.with(|c| *c.borrow_mut() = prev);
    }
}

/// Mark what the running command changes, once (its first successful shard
/// visit). Called by the shard with its lock held.
pub(crate) fn mark_running(reg: &WatchRegistry) {
    let t = TOUCH.with(|c| c.borrow_mut().take());
    match t {
        None => {}
        Some(Touch::Keys(db, keys)) => {
            for k in &keys {
                reg.touch(db, k);
            }
        }
        Some(Touch::Db(db)) => reg.touch_db(Some(db)),
        Some(Touch::All) => reg.touch_db(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touching_a_watched_key_flags_its_watchers_only() {
        let reg = WatchRegistry::default();
        assert!(!reg.is_active());
        let a = Arc::new(WatchFlag::default());
        let b = Arc::new(WatchFlag::default());
        reg.watch(0, Bytes::from("k"), &a);
        reg.watch(1, Bytes::from("k"), &b);
        assert!(reg.is_active());
        reg.touch(0, b"k");
        assert!(a.is_dirty());
        assert!(!b.is_dirty());
        reg.touch_db(Some(1));
        assert!(b.is_dirty());
        reg.unwatch(&[(0, Bytes::from("k"))], &a);
        reg.unwatch(&[(1, Bytes::from("k"))], &b);
        assert!(!reg.is_active());
    }
}
