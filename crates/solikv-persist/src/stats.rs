//! Persistence counters, read by `INFO persistence`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Shared between the writer thread, background saves and `INFO`.
#[derive(Debug)]
pub struct PersistStats {
    /// Replaying snapshots and logs at startup.
    pub loading: AtomicBool,
    pub rdb_bgsave_in_progress: AtomicBool,
    /// Unix seconds of the last successful snapshot (startup time until then,
    /// as Redis reports).
    pub rdb_last_save_time: AtomicU64,
    pub rdb_last_bgsave_ok: AtomicBool,
    pub rdb_saves: AtomicU64,
    /// Write counter value at the last successful snapshot.
    pub rdb_dirty_at_last_save: AtomicU64,
    pub aof_enabled: AtomicBool,
    pub aof_rewrite_in_progress: AtomicBool,
    pub aof_last_rewrite_ok: AtomicBool,
    pub aof_rewrites: AtomicU64,
    /// Log size (base + incremental files) right after the last rewrite:
    /// the reference automatic rewrites grow from.
    pub aof_base_size: AtomicU64,
    /// Log size now.
    pub aof_current_size: AtomicU64,
    pub aof_last_write_ok: AtomicBool,
}

impl PersistStats {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            loading: AtomicBool::new(false),
            rdb_bgsave_in_progress: AtomicBool::new(false),
            rdb_last_save_time: AtomicU64::new(now),
            rdb_last_bgsave_ok: AtomicBool::new(true),
            rdb_saves: AtomicU64::new(0),
            rdb_dirty_at_last_save: AtomicU64::new(0),
            aof_enabled: AtomicBool::new(false),
            aof_rewrite_in_progress: AtomicBool::new(false),
            aof_last_rewrite_ok: AtomicBool::new(true),
            aof_rewrites: AtomicU64::new(0),
            aof_base_size: AtomicU64::new(0),
            aof_current_size: AtomicU64::new(0),
            aof_last_write_ok: AtomicBool::new(true),
        }
    }

    /// The `# Persistence` section of `INFO`, given the server's write count.
    pub fn info_section(&self, dirty: u64) -> String {
        let r = Ordering::Relaxed;
        let ok = |b: &AtomicBool| if b.load(r) { "ok" } else { "err" };
        let flag = |b: &AtomicBool| b.load(r) as u8;
        format!(
            "# Persistence\r\n\
             loading:{}\r\n\
             rdb_changes_since_last_save:{}\r\n\
             rdb_bgsave_in_progress:{}\r\n\
             rdb_last_save_time:{}\r\n\
             rdb_last_bgsave_status:{}\r\n\
             rdb_saves:{}\r\n\
             aof_enabled:{}\r\n\
             aof_rewrite_in_progress:{}\r\n\
             aof_last_bgrewrite_status:{}\r\n\
             aof_rewrites:{}\r\n\
             aof_last_write_status:{}\r\n\
             aof_current_size:{}\r\n\
             aof_base_size:{}\r\n",
            flag(&self.loading),
            dirty.saturating_sub(self.rdb_dirty_at_last_save.load(r)),
            flag(&self.rdb_bgsave_in_progress),
            self.rdb_last_save_time.load(r),
            ok(&self.rdb_last_bgsave_ok),
            self.rdb_saves.load(r),
            flag(&self.aof_enabled),
            flag(&self.aof_rewrite_in_progress),
            ok(&self.aof_last_rewrite_ok),
            self.aof_rewrites.load(r),
            ok(&self.aof_last_write_ok),
            self.aof_current_size.load(r),
            self.aof_base_size.load(r),
        )
    }
}

impl Default for PersistStats {
    fn default() -> Self {
        Self::new()
    }
}
