use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

const MAX_AUTH_FAILURES: u32 = 10;
const AUTH_COOLDOWN_SECS: u64 = 30;
const MAX_TRACKED_IPS: usize = 10_000;

/// Per-IP AUTH failure tracker with cooldown and bounded memory.
pub struct AuthFailureTracker {
    failures: RwLock<HashMap<String, (u32, Instant)>>,
}

impl Default for AuthFailureTracker {
    fn default() -> Self {
        Self {
            failures: RwLock::new(HashMap::new()),
        }
    }
}

impl AuthFailureTracker {
    /// Read-only and allocation-free: the REST middleware calls this on *every*
    /// request, so it must never take the write lock or scan the whole map.
    /// Eviction happens on the failure path instead.
    pub fn is_blocked(&self, ip: &str) -> bool {
        let failures = self.failures.read().unwrap();
        match failures.get(ip) {
            Some((count, last_failure)) if *count >= MAX_AUTH_FAILURES => {
                last_failure.elapsed().as_secs() < AUTH_COOLDOWN_SECS
            }
            _ => false,
        }
    }

    pub fn record_failure(&self, ip: &str) {
        let mut failures = self.failures.write().unwrap();
        // Sweep only once the map has actually grown, so the common case stays O(1).
        if failures.len() > MAX_TRACKED_IPS / 2 {
            evict_stale(&mut failures);
        }
        if failures.len() >= MAX_TRACKED_IPS && !failures.contains_key(ip) {
            // Full of live entries. Make room instead of refusing to track the new IP:
            // dropping the record would let anyone who first seeds MAX_TRACKED_IPS
            // addresses (a routed IPv6 /64 is enough) turn the limiter off for everyone.
            evict_lowest_value_batch(&mut failures);
        }
        let now = Instant::now();
        let entry = failures.entry(ip.to_string()).or_insert((0, now));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = now;
    }

    pub fn record_success(&self, ip: &str) {
        let mut failures = self.failures.write().unwrap();
        failures.remove(ip);
    }
}

/// Free a tenth of the table when it is full of entries too fresh for `evict_stale`.
/// Entries not currently serving a cooldown go first, and within each group the
/// least recently active go first — so a flood of fresh IPs cannot lift an active
/// block. Batching keeps the O(n) scan amortized to O(1) per recorded failure.
fn evict_lowest_value_batch(failures: &mut HashMap<String, (u32, Instant)>) {
    let target = (MAX_TRACKED_IPS / 10).min(failures.len());
    if target == 0 {
        return;
    }
    // Ordering on `(is_blocked, last_failure)`: not-blocked sorts before blocked, and
    // older before newer, so the `target`-th smallest is the eviction cutoff.
    let mut ranked: Vec<(bool, Instant)> = failures
        .values()
        .map(|(count, t)| (*count >= MAX_AUTH_FAILURES, *t))
        .collect();
    let (_, cutoff, _) = ranked.select_nth_unstable(target - 1);
    let cutoff = *cutoff;
    failures.retain(|_, (count, t)| (*count >= MAX_AUTH_FAILURES, *t) > cutoff);
}

fn evict_stale(failures: &mut HashMap<String, (u32, Instant)>) {
    failures.retain(|_, (count, t)| {
        if *count >= MAX_AUTH_FAILURES {
            t.elapsed().as_secs() < AUTH_COOLDOWN_SECS * 2
        } else {
            t.elapsed().as_secs() < AUTH_COOLDOWN_SECS * 4
        }
    });
}

/// Compare `provided` to `expected` in time that depends on `expected.len()`, not on
/// whether the lengths match (avoids a password-length oracle).
pub fn constant_time_eq(provided: &[u8], expected: &[u8]) -> bool {
    // Fold the length check in as a 0/1 flag. Casting a `usize` length difference to
    // `u8` would truncate, so any delta that is a multiple of 256 would vanish and a
    // wrong password of the right shape would compare equal.
    let mut diff = u8::from(provided.len() != expected.len());
    for (i, exp) in expected.iter().enumerate() {
        let got = provided.get(i).copied().unwrap_or(0);
        diff |= got ^ *exp;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_same() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secrex"));
        assert!(!constant_time_eq(b"short", b"longerpass"));
        assert!(!constant_time_eq(b"longerpass", b"short"));
    }

    #[test]
    fn eq_rejects_length_delta_that_truncates_to_zero() {
        // A `usize -> u8` cast of the length difference is 0 for any delta that is a
        // multiple of 256, so these cases must be caught by the full-width check.
        let mut trailing_nuls = b"s".to_vec();
        trailing_nuls.extend(std::iter::repeat_n(0u8, 256));
        assert!(!constant_time_eq(b"s", &trailing_nuls));

        let mut padded = b"secret".to_vec();
        padded.extend(std::iter::repeat_n(b'x', 256));
        assert!(!constant_time_eq(&padded, b"secret"));

        assert!(!constant_time_eq(&vec![0u8; 256], b""));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn blocks_after_max_failures_and_resets_on_success() {
        let tracker = AuthFailureTracker::default();
        for _ in 0..MAX_AUTH_FAILURES - 1 {
            tracker.record_failure("10.0.0.1");
        }
        assert!(!tracker.is_blocked("10.0.0.1"));
        tracker.record_failure("10.0.0.1");
        assert!(tracker.is_blocked("10.0.0.1"));
        assert!(!tracker.is_blocked("10.0.0.2"));

        tracker.record_success("10.0.0.1");
        assert!(!tracker.is_blocked("10.0.0.1"));
    }

    #[test]
    fn tracked_ips_stay_bounded_under_unique_ip_flood() {
        let tracker = AuthFailureTracker::default();
        for i in 0..MAX_TRACKED_IPS + 5_000 {
            tracker.record_failure(&format!("10.{}.{}.{}", i >> 16, (i >> 8) & 0xff, i & 0xff));
        }
        assert!(tracker.failures.read().unwrap().len() <= MAX_TRACKED_IPS);
    }

    #[test]
    fn flood_does_not_disable_the_limiter_for_a_new_ip() {
        // Seeding the table full of fresh entries must not stop a later attacker from
        // being blocked -- that would make the memory bound a rate-limit bypass.
        let tracker = AuthFailureTracker::default();
        for i in 0..MAX_TRACKED_IPS {
            tracker.record_failure(&format!("10.{}.{}.{}", i >> 16, (i >> 8) & 0xff, i & 0xff));
        }
        for _ in 0..MAX_AUTH_FAILURES {
            tracker.record_failure("203.0.113.7");
        }
        assert!(tracker.is_blocked("203.0.113.7"));
    }

    #[test]
    fn eviction_prefers_unblocked_entries_over_active_cooldowns() {
        let tracker = AuthFailureTracker::default();
        for _ in 0..MAX_AUTH_FAILURES {
            tracker.record_failure("198.51.100.1");
        }
        assert!(tracker.is_blocked("198.51.100.1"));
        for i in 0..MAX_TRACKED_IPS * 2 {
            tracker.record_failure(&format!("10.{}.{}.{}", i >> 16, (i >> 8) & 0xff, i & 0xff));
        }
        assert!(
            tracker.is_blocked("198.51.100.1"),
            "a flood of single-failure IPs must not clear an active block"
        );
    }
}
