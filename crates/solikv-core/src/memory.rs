//! Memory accounting for `maxmemory` and `INFO memory`.
//!
//! [`Counting`] wraps the process' global allocator and keeps a running
//! total of the bytes allocated — Redis' `used_memory`, which it gets from
//! its own `zmalloc` wrapper the same way. Each thread accumulates its
//! allocations and frees in a thread-local delta and folds it into the shared
//! total once it exceeds [`FLUSH`] bytes either way, so the hot path is a
//! thread-local add and a compare: no shared cache line is written per
//! allocation. The total is thus exact to within `FLUSH` bytes per thread,
//! and [`used`] adds the calling thread's own pending delta, so a thread that
//! just freed memory (by evicting a key) sees it at once.
//!
//! When the server is not built with the wrapper (library use, tests),
//! [`used`] reports 0 and nothing is ever evicted for memory.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

/// Pending delta a thread keeps before publishing it.
const FLUSH: isize = 32 * 1024;

static TOTAL: AtomicIsize = AtomicIsize::new(0);
static ACTIVE: AtomicBool = AtomicBool::new(false);

thread_local! {
    static LOCAL: Cell<isize> = const { Cell::new(0) };
}

#[inline]
fn account(delta: isize) {
    // `try_with`: allocations can happen while the thread's TLS is being
    // torn down; they go straight to the total then.
    let published = LOCAL.try_with(|l| {
        let v = l.get() + delta;
        if !(-FLUSH..=FLUSH).contains(&v) {
            TOTAL.fetch_add(v, Ordering::Relaxed);
            l.set(0);
        } else {
            l.set(v);
        }
    });
    if published.is_err() {
        TOTAL.fetch_add(delta, Ordering::Relaxed);
    }
}

/// A global allocator that counts what it hands out (see the module docs).
pub struct Counting<A>(pub A);

// SAFETY: every method forwards to the wrapped allocator unchanged; the
// accounting touches only a const-initialised thread-local `Cell` (no
// allocation, no destructor) and an atomic.
unsafe impl<A: GlobalAlloc> GlobalAlloc for Counting<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = self.0.alloc(layout);
        if !p.is_null() {
            account(layout.size() as isize);
        }
        p
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = self.0.alloc_zeroed(layout);
        if !p.is_null() {
            account(layout.size() as isize);
        }
        p
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.0.dealloc(ptr, layout);
        account(-(layout.size() as isize));
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = self.0.realloc(ptr, layout, new_size);
        if !p.is_null() {
            account(new_size as isize - layout.size() as isize);
        }
        p
    }
}

/// Record that the process runs with [`Counting`] as its global allocator
/// (the server calls this at startup), making [`used`] meaningful.
pub fn enable() {
    ACTIVE.store(true, Ordering::Relaxed);
}

/// Whether memory is being counted.
pub fn is_enabled() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Bytes currently allocated by the process (0 when not counting).
pub fn used() -> usize {
    if !is_enabled() {
        return 0;
    }
    let local = LOCAL.try_with(|l| l.get()).unwrap_or(0);
    (TOTAL.load(Ordering::Relaxed) + local).max(0) as usize
}

/// Parse a memory amount the way Redis' config does: `100`, `1kb`, `1k`
/// (1000), `64mb`, `2gb`… (case-insensitive).
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mult): (&str, u64) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1 << 30)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1 << 20)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1 << 10)
    } else if let Some(n) = s.strip_suffix('g') {
        (n, 1_000_000_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 1_000_000)
    } else if let Some(n) = s.strip_suffix('k') {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    num.trim().parse::<u64>().ok()?.checked_mul(mult)
}

/// `used_memory_human`-style rendering.
pub fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{}B", bytes)
    } else {
        format!("{:.2}{}", v, UNITS[u])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_parse_like_redis() {
        assert_eq!(parse_size("100"), Some(100));
        assert_eq!(parse_size("1kb"), Some(1024));
        assert_eq!(parse_size("1k"), Some(1000));
        assert_eq!(parse_size("64MB"), Some(64 << 20));
        assert_eq!(parse_size("2gb"), Some(2 << 30));
        assert_eq!(parse_size("x"), None);
        assert_eq!(human(1536), "1.50K");
    }

    #[test]
    fn counting_tracks_alloc_and_free() {
        let a = Counting(std::alloc::System);
        let layout = Layout::from_size_align(100_000, 8).unwrap();
        let before = TOTAL.load(Ordering::Relaxed) + LOCAL.with(|l| l.get());
        // SAFETY: plain alloc/dealloc pair with the same layout.
        unsafe {
            let p = a.alloc(layout);
            assert!(!p.is_null());
            let mid = TOTAL.load(Ordering::Relaxed) + LOCAL.with(|l| l.get());
            assert_eq!(mid - before, 100_000);
            a.dealloc(p, layout);
        }
        let after = TOTAL.load(Ordering::Relaxed) + LOCAL.with(|l| l.get());
        assert_eq!(after, before);
    }
}
