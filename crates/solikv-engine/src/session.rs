//! Per-connection state the engine needs: the selected database, the
//! `MULTI` queue, watched keys, `CLIENT` metadata and reply suppression.
//!
//! The RESP server owns one [`Session`] per connection and runs every
//! command through [`CommandEngine::execute_session`]. The engine's plain
//! [`CommandEngine::execute`] (REST, scripts, replay, tests) has no session:
//! it runs in database 0, or in whatever database the calling command set.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use solikv_core::{now_millis, CommandResponse};

use crate::watch::WatchFlag;

/// What `CLIENT LIST` shows about a connection. Shared between the
/// connection (which updates it) and the registry (which lists it).
#[derive(Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    pub laddr: String,
    created: Instant,
    name: Mutex<Option<Bytes>>,
    lib: Mutex<(Option<Bytes>, Option<Bytes>)>,
    db: AtomicU32,
    /// Unix ms of the last command batch.
    last_active: AtomicU64,
    last_cmd: Mutex<String>,
    /// Subscriptions, for `CLIENT LIST`'s `sub`/`psub`.
    pub subs: AtomicU32,
    pub psubs: AtomicU32,
    /// Queued commands in an open `MULTI`, or -1.
    multi: AtomicI64,
    /// Commands this connection ran (`INFO stats`). Per connection, so the
    /// hot path bumps a counter no other core writes.
    commands: AtomicU64,
    killed: AtomicBool,
    kill: tokio::sync::Notify,
    /// Protocol version, for `CLIENT LIST`.
    resp: AtomicU32,
    /// ACL user, for `CLIENT LIST`.
    user: Mutex<String>,
    /// Out-of-band delivery (tracking invalidations) to the connection.
    push: Mutex<Option<tokio::sync::mpsc::UnboundedSender<crate::tracking::Push>>>,
    /// `CLIENT TRACKING … REDIRECT` target, for `CLIENT LIST` (-1: none).
    redir: AtomicI64,
}

impl ClientInfo {
    pub fn name(&self) -> Option<Bytes> {
        self.name.lock().clone()
    }

    /// Record activity (once per command batch of `n` commands).
    pub fn touch(&self, last_cmd: &str, n: usize) {
        self.commands.fetch_add(n as u64, Ordering::Relaxed);
        self.last_active.store(now_millis(), Ordering::Relaxed);
        let mut c = self.last_cmd.lock();
        if c.as_str() != last_cmd {
            c.clear();
            c.push_str(last_cmd);
        }
    }

    /// Where this connection receives pushes (set by the connection loop).
    pub fn set_push(&self, tx: tokio::sync::mpsc::UnboundedSender<crate::tracking::Push>) {
        *self.push.lock() = Some(tx);
    }

    pub fn user_name(&self) -> String {
        self.user.lock().clone()
    }

    pub fn is_killed(&self) -> bool {
        self.killed.load(Ordering::Acquire)
    }

    /// Resolves once `CLIENT KILL` targets this client.
    pub async fn killed(&self) {
        loop {
            let notified = self.kill.notified();
            if self.is_killed() {
                return;
            }
            notified.await;
        }
    }

    fn kill(&self) {
        self.killed.store(true, Ordering::Release);
        self.kill.notify_waiters();
    }

    /// One `CLIENT LIST` line (without the trailing newline).
    pub fn describe(&self) -> String {
        let now = now_millis();
        let age = self.created.elapsed().as_secs();
        let idle = now.saturating_sub(self.last_active.load(Ordering::Relaxed)) / 1000;
        let name = self
            .name()
            .map(|n| String::from_utf8_lossy(&n).to_string())
            .unwrap_or_default();
        let (lib_name, lib_ver) = {
            let l = self.lib.lock();
            (
                l.0.as_ref()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_default(),
                l.1.as_ref()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_default(),
            )
        };
        let subs = self.subs.load(Ordering::Relaxed);
        let psubs = self.psubs.load(Ordering::Relaxed);
        let multi = self.multi.load(Ordering::Relaxed);
        let mut flags = String::new();
        if multi >= 0 {
            flags.push('x');
        }
        if subs + psubs > 0 {
            flags.push('P');
        }
        if flags.is_empty() {
            flags.push('N');
        }
        format!(
            "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} \
             ssub=0 multi={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 \
             tot-mem=0 events=r cmd={} user={} redir={} resp={} lib-name={} lib-ver={}",
            self.id,
            self.addr,
            self.laddr,
            name,
            age,
            idle,
            flags,
            self.db.load(Ordering::Relaxed),
            subs,
            psubs,
            multi,
            self.last_cmd.lock().to_lowercase(),
            self.user.lock(),
            self.redir.load(Ordering::Relaxed),
            self.resp.load(Ordering::Relaxed),
            lib_name,
            lib_ver,
        )
    }
}

/// Every connected client (weakly: a connection's drop unregisters it).
#[derive(Default)]
pub struct ClientRegistry {
    next_id: AtomicU64,
    clients: Mutex<BTreeMap<u64, Weak<ClientInfo>>>,
    /// Commands of connections that have closed.
    closed_commands: AtomicU64,
    /// `CONFIG RESETSTAT` baseline.
    stats_base: AtomicU64,
}

impl ClientRegistry {
    pub fn register(&self, addr: String, laddr: String) -> Arc<ClientInfo> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let info = Arc::new(ClientInfo {
            id,
            addr,
            laddr,
            created: Instant::now(),
            name: Mutex::new(None),
            lib: Mutex::new((None, None)),
            db: AtomicU32::new(0),
            last_active: AtomicU64::new(now_millis()),
            last_cmd: Mutex::new(String::from("NULL")),
            subs: AtomicU32::new(0),
            psubs: AtomicU32::new(0),
            multi: AtomicI64::new(-1),
            commands: AtomicU64::new(0),
            killed: AtomicBool::new(false),
            kill: tokio::sync::Notify::new(),
            resp: AtomicU32::new(2),
            user: Mutex::new("default".to_string()),
            push: Mutex::new(None),
            redir: AtomicI64::new(-1),
        });
        self.clients.lock().insert(id, Arc::downgrade(&info));
        info
    }

    fn unregister(&self, info: &ClientInfo) {
        self.closed_commands
            .fetch_add(info.commands.load(Ordering::Relaxed), Ordering::Relaxed);
        self.clients.lock().remove(&info.id);
    }

    /// Commands processed since startup (or the last `CONFIG RESETSTAT`).
    pub fn commands_processed(&self) -> u64 {
        let live: u64 = self
            .list()
            .iter()
            .map(|c| c.commands.load(Ordering::Relaxed))
            .sum();
        (self.closed_commands.load(Ordering::Relaxed) + live)
            .saturating_sub(self.stats_base.load(Ordering::Relaxed))
    }

    pub fn connections_received(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    pub fn reset_stats(&self) {
        let now = self.commands_processed() + self.stats_base.load(Ordering::Relaxed);
        self.stats_base.store(now, Ordering::Relaxed);
    }

    /// Live clients, oldest first.
    pub fn list(&self) -> Vec<Arc<ClientInfo>> {
        let mut m = self.clients.lock();
        m.retain(|_, w| w.strong_count() > 0);
        m.values().filter_map(|w| w.upgrade()).collect()
    }

    pub fn count(&self) -> usize {
        self.clients.lock().len()
    }

    pub fn get(&self, id: u64) -> Option<Arc<ClientInfo>> {
        self.clients.lock().get(&id).and_then(|w| w.upgrade())
    }

    /// Deliver `push` to client `id`, if it is connected and listening.
    pub fn push(&self, id: u64, push: crate::tracking::Push) {
        if let Some(c) = self.get(id) {
            if let Some(tx) = c.push.lock().as_ref() {
                let _ = tx.send(push);
            }
        }
    }

    /// Kill every client authenticated as `user` (`ACL DELUSER`).
    pub fn kill_user(&self, user: &str) {
        for c in self.list() {
            if *c.user.lock() == user {
                c.kill();
            }
        }
    }
}

/// `CLIENT REPLY` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMode {
    On,
    Off,
    /// Suppress the next command's reply, then back to `On`.
    Skip,
}

/// One connection's state.
pub struct Session {
    pub db: u32,
    pub info: Arc<ClientInfo>,
    registry: Arc<ClientRegistry>,
    /// Open `MULTI`: queued `(name, args)`.
    pub(crate) multi: Option<Vec<(String, Vec<Bytes>)>>,
    /// A queued command was rejected: `EXEC` must abort.
    pub(crate) multi_failed: bool,
    pub(crate) watch_flag: Option<Arc<WatchFlag>>,
    pub(crate) watched: Vec<(u32, Bytes)>,
    /// Registry the watches are in, to remove them on drop.
    pub(crate) watch_owner: Option<Arc<crate::ShardManager>>,
    pub(crate) reply: ReplyMode,
    /// The command just run must not be answered (`CLIENT REPLY OFF|SKIP`).
    pub(crate) suppress_current: bool,
    /// Protocol version (`HELLO 2|3`).
    pub resp: u8,
    /// The ACL user this connection runs as, read at `acl_version`.
    pub user: Arc<crate::acl::AclUser>,
    pub(crate) acl_version: u64,
    /// `CLIENT TRACKING` options, when on.
    pub(crate) tracking: Option<crate::tracking::TrackingOpts>,
    /// `CLIENT CACHING yes|no`, for the next command only.
    pub(crate) caching: Option<bool>,
    /// Stops tracking when the connection goes.
    pub(crate) tracking_owner: Option<Arc<crate::tracking::Tracking>>,
}

impl Session {
    pub(crate) fn new(
        info: Arc<ClientInfo>,
        registry: Arc<ClientRegistry>,
        user: Arc<crate::acl::AclUser>,
        acl_version: u64,
    ) -> Self {
        Self {
            db: 0,
            info,
            registry,
            multi: None,
            multi_failed: false,
            watch_flag: None,
            watched: Vec::new(),
            watch_owner: None,
            reply: ReplyMode::On,
            suppress_current: false,
            resp: 2,
            user,
            acl_version,
            tracking: None,
            caching: None,
            tracking_owner: None,
        }
    }

    /// Run as `user` from now on (`AUTH`, `HELLO … AUTH`).
    pub fn set_user(&mut self, user: Arc<crate::acl::AclUser>, version: u64) {
        *self.info.user.lock() = user.name.clone();
        self.user = user;
        self.acl_version = version;
    }

    pub(crate) fn set_redir(&self, redir: Option<u64>) {
        self.info
            .redir
            .store(redir.map_or(-1, |r| r as i64), Ordering::Relaxed);
    }

    /// Switch protocol (`HELLO`).
    pub fn set_resp(&mut self, proto: u8) {
        self.resp = proto;
        self.info.resp.store(proto as u32, Ordering::Relaxed);
    }

    pub fn in_multi(&self) -> bool {
        self.multi.is_some()
    }

    /// Whether the reply to the command just executed is to be sent.
    pub fn reply_allowed(&mut self) -> bool {
        if std::mem::take(&mut self.suppress_current) {
            return false;
        }
        match self.reply {
            ReplyMode::On => true,
            ReplyMode::Off => false,
            ReplyMode::Skip => {
                self.reply = ReplyMode::On;
                false
            }
        }
    }

    pub(crate) fn set_db(&mut self, db: u32) {
        self.db = db;
        self.info.db.store(db, Ordering::Relaxed);
    }

    pub(crate) fn set_multi_len(&self, n: Option<usize>) {
        self.info
            .multi
            .store(n.map_or(-1, |n| n as i64), Ordering::Relaxed);
    }

    pub(crate) fn set_name(&self, name: Option<Bytes>) {
        *self.info.name.lock() = name;
    }

    pub(crate) fn set_lib(&self, name: Option<Bytes>, ver: Option<Bytes>) {
        let mut l = self.info.lib.lock();
        if name.is_some() {
            l.0 = name;
        }
        if ver.is_some() {
            l.1 = ver;
        }
    }

    /// Forget every watched key.
    pub(crate) fn unwatch_all(&mut self) {
        if let (Some(flag), Some(owner)) = (&self.watch_flag, &self.watch_owner) {
            owner.watch().unwatch(&self.watched, flag);
        }
        self.watched.clear();
        self.watch_flag = None;
    }

    pub(crate) fn kill_matching(
        &self,
        mut pred: impl FnMut(&ClientInfo) -> bool,
        skip_me: bool,
    ) -> i64 {
        let mut n = 0;
        for c in self.registry.list() {
            if skip_me && c.id == self.info.id {
                continue;
            }
            if pred(&c) {
                c.kill();
                n += 1;
            }
        }
        n
    }

    pub(crate) fn registry(&self) -> &Arc<ClientRegistry> {
        &self.registry
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.tracking.take().is_some() {
            if let Some(t) = &self.tracking_owner {
                t.disable(self.info.id);
            }
        }
        self.unwatch_all();
        self.registry.unregister(&self.info);
    }
}

/// `+QUEUED`.
pub(crate) fn queued() -> CommandResponse {
    CommandResponse::Queued
}
