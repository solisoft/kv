//! `client-output-buffer-limit`: how much a client may have waiting to be
//! written before it is disconnected — Redis' three classes, with its
//! defaults and its `CONFIG SET` syntax (`<class> <hard> <soft> <seconds>`,
//! any number of classes, sizes with units).
//!
//! A client is over its limit when what it has pending exceeds `hard`, or
//! has exceeded `soft` continuously for more than `soft_secs`; 0 disables
//! either. Normal clients have no limit by default, like Redis.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// A client class with its own limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Normal = 0,
    Replica = 1,
    Pubsub = 2,
}

impl Class {
    fn parse(s: &str) -> Option<Class> {
        match s.to_ascii_lowercase().as_str() {
            "normal" => Some(Class::Normal),
            "replica" | "slave" => Some(Class::Replica),
            "pubsub" => Some(Class::Pubsub),
            _ => None,
        }
    }

    /// The name `CONFIG GET` reports (Redis still says `slave`).
    fn name(self) -> &'static str {
        match self {
            Class::Normal => "normal",
            Class::Replica => "slave",
            Class::Pubsub => "pubsub",
        }
    }
}

/// One class's limit, in bytes and seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub hard: u64,
    pub soft: u64,
    pub soft_secs: u64,
}

impl Limit {
    /// Whether a client with `pending` bytes waiting is over this limit.
    /// `soft_since` is the client's own record of when it went over the
    /// soft limit, kept between calls (cleared when it drops below).
    pub fn exceeded(&self, pending: usize, soft_since: &mut Option<Instant>) -> bool {
        let pending = pending as u64;
        if self.hard > 0 && pending > self.hard {
            return true;
        }
        if self.soft > 0 && pending > self.soft {
            // As Redis: the first time over only starts the clock.
            let Some(since) = *soft_since else {
                *soft_since = Some(Instant::now());
                return false;
            };
            return since.elapsed().as_secs_f64() > self.soft_secs as f64;
        }
        *soft_since = None;
        false
    }

    /// No limit at all (nothing to check).
    pub fn is_unlimited(&self) -> bool {
        self.hard == 0 && self.soft == 0
    }
}

const MB: u64 = 1024 * 1024;

const DEFAULTS: [Limit; 3] = [
    Limit {
        hard: 0,
        soft: 0,
        soft_secs: 0,
    },
    Limit {
        hard: 256 * MB,
        soft: 64 * MB,
        soft_secs: 60,
    },
    Limit {
        hard: 32 * MB,
        soft: 8 * MB,
        soft_secs: 60,
    },
];

/// The limits of every class, changeable at runtime.
pub struct OutputLimits {
    v: [[AtomicU64; 3]; 3],
}

impl Default for OutputLimits {
    fn default() -> Self {
        let l = |c: usize| {
            [
                AtomicU64::new(DEFAULTS[c].hard),
                AtomicU64::new(DEFAULTS[c].soft),
                AtomicU64::new(DEFAULTS[c].soft_secs),
            ]
        };
        Self {
            v: [l(0), l(1), l(2)],
        }
    }
}

impl OutputLimits {
    /// `class`'s limit (three relaxed loads).
    #[inline]
    pub fn get(&self, class: Class) -> Limit {
        let v = &self.v[class as usize];
        Limit {
            hard: v[0].load(Ordering::Relaxed),
            soft: v[1].load(Ordering::Relaxed),
            soft_secs: v[2].load(Ordering::Relaxed),
        }
    }

    pub fn set(&self, class: Class, l: Limit) {
        let v = &self.v[class as usize];
        v[0].store(l.hard, Ordering::Relaxed);
        v[1].store(l.soft, Ordering::Relaxed);
        v[2].store(l.soft_secs, Ordering::Relaxed);
    }

    /// Parse `<class> <hard> <soft> <seconds> …` (the `CONFIG SET` value).
    pub fn parse(spec: &str) -> Result<Vec<(Class, Limit)>, String> {
        let words: Vec<&str> = spec.split_ascii_whitespace().collect();
        if words.is_empty() || !words.len().is_multiple_of(4) {
            return Err("Wrong number of arguments in buffer limit configuration.".into());
        }
        let size = |w: &str| {
            solikv_core::memory::parse_size(w).ok_or_else(|| {
                "Error in hard, soft or soft_seconds setting in buffer limit configuration."
                    .to_string()
            })
        };
        words
            .chunks(4)
            .map(|g| {
                let class = Class::parse(g[0]).ok_or_else(|| {
                    "Invalid client class specified in buffer limit configuration.".to_string()
                })?;
                let soft_secs = g[3].parse::<u64>().map_err(|_| {
                    "Error in hard, soft or soft_seconds setting in buffer limit configuration."
                        .to_string()
                })?;
                Ok((
                    class,
                    Limit {
                        hard: size(g[1])?,
                        soft: size(g[2])?,
                        soft_secs,
                    },
                ))
            })
            .collect()
    }

    /// What `CONFIG GET client-output-buffer-limit` answers.
    pub fn render(&self) -> String {
        [Class::Normal, Class::Replica, Class::Pubsub]
            .iter()
            .map(|&c| {
                let l = self.get(c);
                format!("{} {} {} {}", c.name(), l.hard, l.soft, l.soft_secs)
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The defaults, rendered (for `CONFIG REWRITE`).
    pub fn default_render() -> String {
        OutputLimits::default().render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_renders_like_redis() {
        let l = OutputLimits::default();
        assert_eq!(
            l.render(),
            "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
        );
        for (c, v) in OutputLimits::parse("normal 1mb 512kb 10 pubsub 0 0 0").unwrap() {
            l.set(c, v);
        }
        assert_eq!(
            l.render(),
            "normal 1048576 524288 10 slave 268435456 67108864 60 pubsub 0 0 0"
        );
        assert!(OutputLimits::parse("normal 1 2").is_err());
        assert!(OutputLimits::parse("nobody 1 2 3").is_err());
        assert!(OutputLimits::parse("normal x 2 3").is_err());
        assert_eq!(
            OutputLimits::parse("replica 1 2 3").unwrap()[0].0,
            Class::Replica
        );
    }

    #[test]
    fn hard_is_immediate_soft_needs_its_seconds() {
        let mut since = None;
        let l = Limit {
            hard: 100,
            soft: 10,
            soft_secs: 0,
        };
        assert!(!l.exceeded(5, &mut since));
        assert!(l.exceeded(101, &mut since));
        // Over the soft limit: the first look only starts the clock.
        assert!(!l.exceeded(50, &mut since));
        assert!(since.is_some());
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(l.exceeded(50, &mut since));
        // Back under it: the clock stops.
        assert!(!l.exceeded(5, &mut since));
        assert!(since.is_none());
        let none = Limit {
            hard: 0,
            soft: 0,
            soft_secs: 0,
        };
        assert!(none.is_unlimited());
        assert!(!none.exceeded(usize::MAX, &mut since));
    }
}
