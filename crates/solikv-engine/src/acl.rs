//! ACL: users, their passwords, and what each may run and touch.
//!
//! Redis 7's model: a user is `on` or `off`, has SHA-256 password hashes (or
//! `nopass`), a set of allowed commands (by name, `+cmd|sub`, or category
//! `+@read`), key patterns (`~pat`, `%R~pat`, `%W~pat`) and channel patterns
//! (`&pat`). The `default` user is the one `AUTH <password>` and
//! `requirepass` mean.
//!
//! The check is cheap where it matters: a user allowed everything (the
//! default user, unless an operator restricts it) is marked `unrestricted`
//! and the connection skips the check entirely; otherwise the command is
//! one bit test in a bitset indexed by command id, and keys are matched
//! only for users that have key patterns.

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use solikv_core::CommandResponse;

use crate::commands::{
    self, Cmd, CommandSpec, F_ADMIN, F_BLOCKING, F_FAST, F_PUBSUB, F_READONLY, F_WRITE,
};

// ---------------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------------

/// Redis' ACL categories, in `ACL CAT` order.
pub const CATEGORIES: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

fn cat_bit(name: &str) -> Option<u32> {
    CATEGORIES.iter().position(|c| *c == name).map(|i| 1 << i)
}

const STRING: &[&str] = &[
    "GET",
    "SET",
    "SETNX",
    "SETEX",
    "PSETEX",
    "GETSET",
    "GETDEL",
    "GETEX",
    "MGET",
    "MSET",
    "MSETNX",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "INCRBYFLOAT",
    "APPEND",
    "STRLEN",
    "GETRANGE",
    "SUBSTR",
    "SETRANGE",
    "LCS",
];
const BITMAP: &[&str] = &[
    "SETBIT",
    "GETBIT",
    "BITCOUNT",
    "BITOP",
    "BITPOS",
    "BITFIELD",
    "BITFIELD_RO",
];
const KEYSPACE: &[&str] = &[
    "DEL",
    "UNLINK",
    "EXISTS",
    "TOUCH",
    "TYPE",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "EXPIRETIME",
    "PEXPIRETIME",
    "TTL",
    "PTTL",
    "PERSIST",
    "RENAME",
    "RENAMENX",
    "MOVE",
    "KEYS",
    "SCAN",
    "RANDOMKEY",
    "OBJECT",
    "DUMP",
    "RESTORE",
    "RESTORE-ASKING",
    "MIGRATE",
    "SORT",
    "SORT_RO",
    "COPY",
    "DBSIZE",
    "FLUSHDB",
    "FLUSHALL",
    "SWAPDB",
];
const CONNECTION: &[&str] = &[
    "PING", "ECHO", "AUTH", "HELLO", "QUIT", "RESET", "SELECT", "CLIENT",
];
const TRANSACTION: &[&str] = &["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"];
const SCRIPTING: &[&str] = &[
    "EVAL",
    "EVALSHA",
    "EVAL_RO",
    "EVALSHA_RO",
    "SCRIPT",
    "FCALL",
    "FCALL_RO",
    "FUNCTION",
];
/// `@dangerous` beyond the `admin` commands.
const DANGEROUS: &[&str] = &[
    "FLUSHDB",
    "FLUSHALL",
    "KEYS",
    "SWAPDB",
    "SORT",
    "RESTORE",
    "RESTORE-ASKING",
    "MIGRATE",
    "INFO",
    "ROLE",
    "CLIENT",
    "CONFIG",
    "CLUSTER",
    "DEBUG",
    "MONITOR",
    "ACL",
    "SHUTDOWN",
    "SAVE",
    "BGSAVE",
    "BGREWRITEAOF",
    "REPLICAOF",
    "SLAVEOF",
    "SLOWLOG",
    "LATENCY",
    "MEMORY",
    "FUNCTION",
    "LASTSAVE",
];
const ADMIN_EXTRA: &[&str] = &[
    "CONFIG",
    "DEBUG",
    "MONITOR",
    "SLOWLOG",
    "LATENCY",
    "ACL",
    "SAVE",
    "BGSAVE",
    "BGREWRITEAOF",
    "SHUTDOWN",
    "REPLICAOF",
    "SLAVEOF",
    "LASTSAVE",
    "CLIENT",
];

/// The categories of a command.
pub fn categories_of(spec: &CommandSpec) -> u32 {
    let n = spec.name;
    let mut c = 0u32;
    let mut add = |name: &str| c |= cat_bit(name).unwrap_or(0);
    if spec.has(F_READONLY) {
        add("read");
    }
    if spec.has(F_WRITE) {
        add("write");
    }
    if spec.has(F_FAST) {
        add("fast");
    } else {
        add("slow");
    }
    if spec.has(F_BLOCKING) {
        add("blocking");
    }
    if spec.has(F_ADMIN) || ADMIN_EXTRA.contains(&n) {
        add("admin");
        add("dangerous");
    }
    if DANGEROUS.contains(&n) {
        add("dangerous");
    }
    if spec.has(F_PUBSUB) || n.contains("SUBSCRIBE") || n == "SPUBLISH" {
        add("pubsub");
    }
    if STRING.contains(&n) {
        add("string");
    }
    if BITMAP.contains(&n) {
        add("bitmap");
    }
    if KEYSPACE.contains(&n) {
        add("keyspace");
    }
    if CONNECTION.contains(&n) {
        add("connection");
    }
    if TRANSACTION.contains(&n) {
        add("transaction");
    }
    if SCRIPTING.contains(&n) {
        add("scripting");
    }
    let first = n.as_bytes()[0];
    match first {
        b'L' | b'R'
            if n.starts_with("LPUSH")
                || n.starts_with("RPUSH")
                || matches!(
                    n,
                    "LPOP"
                        | "RPOP"
                        | "LLEN"
                        | "LINDEX"
                        | "LSET"
                        | "LRANGE"
                        | "LINSERT"
                        | "LREM"
                        | "LTRIM"
                        | "LPOS"
                        | "LMOVE"
                        | "RPOPLPUSH"
                        | "LMPOP"
                ) =>
        {
            add("list")
        }
        _ => {}
    }
    if matches!(n, "BLPOP" | "BRPOP" | "BLMOVE" | "BRPOPLPUSH" | "BLMPOP") {
        add("list");
    }
    if n.starts_with('H') && !matches!(n, "HELLO") {
        add("hash");
    }
    if (n.starts_with('S') && !STRING.contains(&n) && !BITMAP.contains(&n))
        && matches!(
            n,
            "SADD"
                | "SREM"
                | "SMEMBERS"
                | "SISMEMBER"
                | "SCARD"
                | "SPOP"
                | "SRANDMEMBER"
                | "SMOVE"
                | "SINTER"
                | "SUNION"
                | "SDIFF"
                | "SINTERSTORE"
                | "SUNIONSTORE"
                | "SDIFFSTORE"
                | "SMISMEMBER"
                | "SINTERCARD"
                | "SSCAN"
        )
    {
        add("set");
    }
    if n.starts_with('Z') || n.starts_with("BZ") {
        add("sortedset");
    }
    if n.starts_with("GEO") {
        add("geo");
    }
    if n.starts_with('X') {
        add("stream");
    }
    if n.starts_with("PF") {
        add("hyperloglog");
    }
    c
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

const WORDS: usize = 8; // 512 command ids

#[derive(Clone, Debug)]
struct KeyPattern {
    pattern: String,
    read: bool,
    write: bool,
}

/// One ACL user.
#[derive(Clone, Debug)]
pub struct AclUser {
    pub name: String,
    enabled: bool,
    nopass: bool,
    passwords: Vec<[u8; 32]>,
    allowed: [u64; WORDS],
    /// `+cmd|sub`: allowed first arguments of otherwise-denied commands.
    allowed_sub: Vec<(Cmd, String)>,
    /// Command rules as given, for `ACL LIST`/`GETUSER`.
    cmd_rules: Vec<String>,
    keys: Vec<KeyPattern>,
    channels: Vec<String>,
    all_channels: bool,
    /// Everything allowed: no check needed.
    unrestricted: bool,
    /// Redis 7 selectors, `(~key* +cmd …)`: further sets of command, key
    /// and channel permissions. A command is allowed when the user's own
    /// permissions or any one selector allow all of it.
    selectors: Vec<AclUser>,
}

impl AclUser {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            allowed: [0; WORDS],
            allowed_sub: Vec::new(),
            cmd_rules: vec!["-@all".into()],
            keys: Vec::new(),
            channels: Vec::new(),
            all_channels: false,
            unrestricted: false,
            selectors: Vec::new(),
        }
    }

    /// A selector's permissions from its rules (the text between the
    /// parentheses): commands, keys and channels only.
    fn selector(rules: &str) -> Result<Self, String> {
        let mut sel = AclUser::new("");
        sel.enabled = true;
        for rule in rules.split_ascii_whitespace() {
            let lower = rule.to_ascii_lowercase();
            let allowed = rule.starts_with(['+', '-', '~', '%', '&'])
                || matches!(
                    lower.as_str(),
                    "allkeys"
                        | "resetkeys"
                        | "allchannels"
                        | "resetchannels"
                        | "allcommands"
                        | "nocommands"
                );
            if !allowed {
                return Err(format!("Syntax error in selector: '{}'", rule));
            }
            sel.apply(rule)?;
        }
        Ok(sel)
    }

    /// A selector's rules, as `ACL LIST` writes them inside parentheses.
    fn selector_rules(&self) -> String {
        [self.keys_rule(), self.channels_rule(), self.commands_rule()]
            .into_iter()
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The `default` user of a fresh server: everything, and `nopass`
    /// unless `requirepass` sets one.
    fn default_user(password: Option<&str>) -> Self {
        let mut u = Self::new("default");
        let mut rules = vec!["on", "~*", "&*", "+@all"];
        let pw;
        match password {
            Some(p) => {
                pw = format!(">{}", p);
                rules.push(&pw);
            }
            None => rules.push("nopass"),
        }
        for r in rules {
            let _ = u.apply(r);
        }
        u
    }

    #[inline]
    pub fn unrestricted(&self) -> bool {
        self.unrestricted
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// May read every key (`~*` or `%R~*`): what `SORT`'s `BY` and `GET`
    /// patterns need, since the keys they read are known only at run time.
    pub fn reads_all_keys(&self) -> bool {
        self.keys.iter().any(|k| k.pattern == "*" && k.read)
    }

    fn refresh(&mut self) {
        let all_cmds =
            (0..commands::all().len()).all(|i| self.allowed[i / 64] & (1 << (i % 64)) != 0);
        let all_keys = self
            .keys
            .iter()
            .any(|k| k.pattern == "*" && k.read && k.write);
        self.unrestricted = all_cmds && all_keys && self.all_channels;
    }

    fn set_cmd(&mut self, id: usize, on: bool) {
        if on {
            self.allowed[id / 64] |= 1 << (id % 64);
        } else {
            self.allowed[id / 64] &= !(1 << (id % 64));
        }
    }

    fn cmd_allowed(&self, id: Cmd) -> bool {
        let i = id as usize;
        i < WORDS * 64 && self.allowed[i / 64] & (1 << (i % 64)) != 0
    }

    /// Apply one `ACL SETUSER` rule.
    pub fn apply(&mut self, rule: &str) -> Result<(), String> {
        let lower = rule.to_ascii_lowercase();
        match lower.as_str() {
            "on" => self.enabled = true,
            "off" => self.enabled = false,
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
            }
            "resetpass" => {
                self.nopass = false;
                self.passwords.clear();
            }
            "allkeys" => self.apply("~*")?,
            "resetkeys" => self.keys.clear(),
            "allchannels" => self.apply("&*")?,
            "resetchannels" => {
                self.channels.clear();
                self.all_channels = false;
            }
            "allcommands" => self.apply("+@all")?,
            "nocommands" => self.apply("-@all")?,
            "reset" => {
                *self = AclUser::new(&self.name.clone());
                self.apply("resetchannels")?;
            }
            "clearselectors" => self.selectors.clear(),
            "sanitize-payload" | "skip-sanitize-payload" => {}
            _ => {
                if let Some(p) = rule.strip_prefix('>') {
                    let h = sha256(p.as_bytes());
                    if !self.passwords.contains(&h) {
                        self.passwords.push(h);
                    }
                    self.nopass = false;
                } else if let Some(p) = rule.strip_prefix('<') {
                    let h = sha256(p.as_bytes());
                    if !self.passwords.contains(&h) {
                        return Err("no such password".into());
                    }
                    self.passwords.retain(|x| *x != h);
                } else if let Some(hex) = rule.strip_prefix('#') {
                    let h = parse_hash(hex).ok_or("The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters")?;
                    if !self.passwords.contains(&h) {
                        self.passwords.push(h);
                    }
                    self.nopass = false;
                } else if let Some(hex) = rule.strip_prefix('!') {
                    let h = parse_hash(hex).ok_or("The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters")?;
                    self.passwords.retain(|x| *x != h);
                } else if let Some(p) = rule.strip_prefix('~') {
                    self.keys.push(KeyPattern {
                        pattern: p.to_string(),
                        read: true,
                        write: true,
                    });
                } else if let Some(rest) = rule.strip_prefix('%') {
                    let (perm, pat) = rest.split_once('~').ok_or("Syntax error")?;
                    let perm = perm.to_ascii_uppercase();
                    let read = perm.contains('R');
                    let write = perm.contains('W');
                    if perm.is_empty() || perm.chars().any(|c| c != 'R' && c != 'W') {
                        return Err("Syntax error".into());
                    }
                    self.keys.push(KeyPattern {
                        pattern: pat.to_string(),
                        read,
                        write,
                    });
                } else if let Some(p) = rule.strip_prefix('&') {
                    if p == "*" {
                        self.all_channels = true;
                        self.channels.clear();
                    } else {
                        self.channels.push(p.to_string());
                    }
                } else if let Some(c) = lower.strip_prefix("+@") {
                    self.set_category(c, true)?;
                    self.push_cmd_rule(rule);
                } else if let Some(c) = lower.strip_prefix("-@") {
                    self.set_category(c, false)?;
                    self.push_cmd_rule(rule);
                } else if let Some(c) = rule.strip_prefix('+') {
                    match c.split_once('|') {
                        Some((cmd, sub)) => {
                            let spec = commands::lookup(&cmd.to_ascii_uppercase())
                                .ok_or("Unknown command")?;
                            self.allowed_sub.push((spec.id, sub.to_ascii_lowercase()));
                        }
                        None => {
                            let spec = commands::lookup(&c.to_ascii_uppercase())
                                .ok_or("Unknown command")?;
                            self.set_cmd(spec.id as usize, true);
                        }
                    }
                    self.push_cmd_rule(&rule.to_ascii_lowercase());
                } else if let Some(c) = rule.strip_prefix('-') {
                    let spec =
                        commands::lookup(&c.to_ascii_uppercase()).ok_or("Unknown command")?;
                    self.set_cmd(spec.id as usize, false);
                    self.allowed_sub.retain(|(id, _)| *id != spec.id);
                    self.push_cmd_rule(&rule.to_ascii_lowercase());
                } else if let Some(inner) = rule.strip_prefix('(') {
                    let inner = inner
                        .strip_suffix(')')
                        .ok_or("Unmatched parenthesis in acl selector")?;
                    self.selectors.push(AclUser::selector(inner)?);
                } else {
                    return Err("Syntax error".into());
                }
            }
        }
        self.refresh();
        Ok(())
    }

    fn push_cmd_rule(&mut self, rule: &str) {
        let lower = rule.to_ascii_lowercase();
        if lower == "+@all" || lower == "-@all" {
            self.cmd_rules.clear();
        }
        self.cmd_rules.push(lower);
    }

    fn set_category(&mut self, cat: &str, on: bool) -> Result<(), String> {
        if cat == "all" {
            for i in 0..commands::all().len() {
                self.set_cmd(i, on);
            }
            if !on {
                self.allowed_sub.clear();
            }
            return Ok(());
        }
        let bit = cat_bit(cat).ok_or("Unknown command category")?;
        for spec in commands::all() {
            if categories_of(spec) & bit != 0 {
                self.set_cmd(spec.id as usize, on);
            }
        }
        Ok(())
    }

    fn key_allowed(&self, key: &[u8], write: bool) -> bool {
        let key = String::from_utf8_lossy(key);
        self.keys.iter().any(|k| {
            (if write { k.write } else { k.read })
                && solikv_core::store::glob_match_pub(&k.pattern, &key)
        })
    }

    pub fn channel_allowed(&self, channel: &[u8], is_pattern: bool) -> bool {
        if self.all_channels {
            return true;
        }
        let ch = String::from_utf8_lossy(channel);
        self.channels.iter().any(|p| {
            if is_pattern {
                p == &ch
            } else {
                solikv_core::store::glob_match_pub(p, &ch)
            }
        })
    }

    fn password_ok(&self, pass: &[u8]) -> bool {
        if self.nopass {
            return true;
        }
        let h = sha256(pass);
        let mut ok = false;
        for p in &self.passwords {
            ok |= constant_time_eq(p, &h);
        }
        ok
    }

    /// The rules that recreate this user (`ACL LIST`, the ACL file).
    pub fn describe(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];
        parts.push(if self.enabled { "on" } else { "off" }.into());
        if self.nopass {
            parts.push("nopass".into());
        }
        for p in &self.passwords {
            parts.push(format!("#{}", hex(p)));
        }
        parts.push(self.keys_rule());
        parts.push(self.channels_rule());
        parts.push(self.commands_rule());
        for sel in &self.selectors {
            parts.push(format!("({})", sel.selector_rules()));
        }
        parts.retain(|p| !p.is_empty());
        parts.join(" ")
    }

    fn keys_rule(&self) -> String {
        if self.keys.is_empty() {
            return "resetkeys".into();
        }
        self.keys
            .iter()
            .map(|k| match (k.read, k.write) {
                (true, true) => format!("~{}", k.pattern),
                (true, false) => format!("%R~{}", k.pattern),
                (false, true) => format!("%W~{}", k.pattern),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn channels_rule(&self) -> String {
        if self.all_channels {
            "&*".into()
        } else if self.channels.is_empty() {
            "resetchannels".into()
        } else {
            self.channels
                .iter()
                .map(|c| format!("&{}", c))
                .collect::<Vec<_>>()
                .join(" ")
        }
    }

    fn commands_rule(&self) -> String {
        let mut s = self.cmd_rules.join(" ");
        for (id, sub) in &self.allowed_sub {
            if let Some(spec) = commands::spec(*id) {
                let r = format!("+{}|{}", spec.name.to_lowercase(), sub);
                if !s.contains(&r) {
                    s.push(' ');
                    s.push_str(&r);
                }
            }
        }
        s
    }

    /// `ACL GETUSER`'s reply (RESP2: a flat array of field, value).
    fn getuser(&self) -> CommandResponse {
        let mut flags = vec![CommandResponse::bulk_string(if self.enabled {
            "on"
        } else {
            "off"
        })];
        if self.nopass {
            flags.push(CommandResponse::bulk_string("nopass"));
        }
        flags.push(CommandResponse::bulk_string("sanitize-payload"));
        CommandResponse::array(vec![
            CommandResponse::bulk_string("flags"),
            CommandResponse::array(flags),
            CommandResponse::bulk_string("passwords"),
            CommandResponse::array(
                self.passwords
                    .iter()
                    .map(|p| CommandResponse::bulk(Bytes::from(hex(p))))
                    .collect(),
            ),
            CommandResponse::bulk_string("commands"),
            CommandResponse::bulk(Bytes::from(self.commands_rule())),
            CommandResponse::bulk_string("keys"),
            CommandResponse::bulk(Bytes::from(if self.keys.is_empty() {
                String::new()
            } else {
                self.keys_rule()
            })),
            CommandResponse::bulk_string("channels"),
            CommandResponse::bulk(Bytes::from(if self.all_channels {
                "&*".to_string()
            } else {
                self.channels
                    .iter()
                    .map(|c| format!("&{}", c))
                    .collect::<Vec<_>>()
                    .join(" ")
            })),
            CommandResponse::bulk_string("selectors"),
            CommandResponse::array(
                self.selectors
                    .iter()
                    .map(|sel| {
                        CommandResponse::array(vec![
                            CommandResponse::bulk_string("commands"),
                            CommandResponse::bulk(Bytes::from(sel.commands_rule())),
                            CommandResponse::bulk_string("keys"),
                            CommandResponse::bulk(Bytes::from(if sel.keys.is_empty() {
                                String::new()
                            } else {
                                sel.keys_rule()
                            })),
                            CommandResponse::bulk_string("channels"),
                            CommandResponse::bulk(Bytes::from(if sel.all_channels {
                                "&*".to_string()
                            } else {
                                sel.channels
                                    .iter()
                                    .map(|c| format!("&{}", c))
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            })),
                        ])
                    })
                    .collect(),
            ),
        ])
    }
}

/// Join a selector given as several arguments — `(~key*` `+get)`, as a
/// shell or an ACL file line splits it — back into one rule, as Redis does.
fn merge_selector_args(args: impl IntoIterator<Item = String>) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut open: Option<String> = None;
    for a in args {
        match open.as_mut() {
            Some(sel) => {
                sel.push(' ');
                sel.push_str(&a);
                if a.ends_with(')') {
                    out.extend(open.take());
                }
            }
            None if a.starts_with('(') && !a.ends_with(')') => open = Some(a),
            None => out.push(a),
        }
    }
    match open {
        Some(sel) => Err(format!(
            "Unmatched parenthesis in acl selector starting at '{}'.",
            sel
        )),
        None => Ok(out),
    }
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn parse_hash(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64
        || !s
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

// ---------------------------------------------------------------------------
// Denials and the ACL log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Context {
    TopLevel,
    Multi,
    Lua,
}

impl Context {
    fn name(self) -> &'static str {
        match self {
            Context::TopLevel => "toplevel",
            Context::Multi => "multi",
            Context::Lua => "lua",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Denied {
    Command(String),
    Key(Bytes),
    Channel(Bytes),
}

impl Denied {
    pub fn message(&self, user: &str) -> String {
        match self {
            Denied::Command(c) => format!(
                "NOPERM User {} has no permissions to run the '{}' command",
                user, c
            ),
            Denied::Key(_) => "NOPERM No permissions to access a key".into(),
            Denied::Channel(_) => "NOPERM No permissions to access a channel".into(),
        }
    }

    fn reason(&self) -> (&'static str, String) {
        match self {
            Denied::Command(c) => ("command", c.clone()),
            Denied::Key(k) => ("key", String::from_utf8_lossy(k).to_string()),
            Denied::Channel(c) => ("channel", String::from_utf8_lossy(c).to_string()),
        }
    }
}

struct LogEntry {
    id: u64,
    count: u64,
    reason: &'static str,
    context: &'static str,
    object: String,
    username: String,
    client_info: String,
    created_ms: u64,
    updated_ms: u64,
}

const ACL_LOG_MAX: usize = 128;

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

/// Every ACL user. Connections hold an `Arc` of their user and re-read it
/// when [`version`](Self::version) moves.
pub struct Acl {
    users: RwLock<BTreeMap<String, Arc<AclUser>>>,
    version: AtomicU64,
    log: Mutex<VecDeque<LogEntry>>,
    log_id: AtomicU64,
    file: RwLock<Option<std::path::PathBuf>>,
}

impl Default for Acl {
    fn default() -> Self {
        let mut users = BTreeMap::new();
        users.insert("default".to_string(), Arc::new(AclUser::default_user(None)));
        Self {
            users: RwLock::new(users),
            version: AtomicU64::new(1),
            log: Mutex::new(VecDeque::new()),
            log_id: AtomicU64::new(0),
            file: RwLock::new(None),
        }
    }
}

pub enum AuthFailure {
    /// `AUTH <password>` with no password configured.
    NoPassword,
    Wrong,
}

impl Acl {
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    fn bump(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    pub fn user(&self, name: &str) -> Option<Arc<AclUser>> {
        self.users.read().get(name).cloned()
    }

    pub fn default_user(&self) -> Arc<AclUser> {
        self.user("default")
            .unwrap_or_else(|| Arc::new(AclUser::default_user(None)))
    }

    /// Whether a new connection starts authenticated (the default user is
    /// on and needs no password).
    pub fn default_auto_auth(&self) -> bool {
        let d = self.default_user();
        d.enabled && d.nopass
    }

    /// `requirepass`: the default user's password (and `CONFIG SET`).
    pub fn set_requirepass(&self, pass: Option<&str>) {
        let mut users = self.users.write();
        let mut d = users
            .get("default")
            .map(|u| (**u).clone())
            .unwrap_or_else(|| AclUser::default_user(None));
        let _ = d.apply("resetpass");
        match pass {
            Some(p) if !p.is_empty() => {
                let _ = d.apply(&format!(">{}", p));
            }
            _ => {
                let _ = d.apply("nopass");
            }
        }
        users.insert("default".into(), Arc::new(d));
        drop(users);
        self.bump();
    }

    /// The default user's first password hash is `requirepass` (`CONFIG
    /// GET requirepass` shows it only as set/unset).
    pub fn has_requirepass(&self) -> bool {
        !self.default_user().nopass
    }

    /// `AUTH [user] pass`.
    pub fn authenticate(
        &self,
        user: Option<&[u8]>,
        pass: &[u8],
    ) -> Result<Arc<AclUser>, AuthFailure> {
        let name = user.map(|u| String::from_utf8_lossy(u).to_string());
        let u = match &name {
            Some(n) => self.user(n).ok_or(AuthFailure::Wrong)?,
            None => {
                let d = self.default_user();
                if d.nopass {
                    return Err(AuthFailure::NoPassword);
                }
                d
            }
        };
        if u.enabled && u.password_ok(pass) {
            Ok(u)
        } else {
            Err(AuthFailure::Wrong)
        }
    }

    /// Check `name args…` for `user`: its own permissions, else any of its
    /// selectors (the denial reported is the one of its own permissions,
    /// as in Redis).
    pub fn check(&self, user: &AclUser, spec: &CommandSpec, args: &[Bytes]) -> Result<(), Denied> {
        match Self::check_perms(user, spec, args) {
            Ok(()) => Ok(()),
            Err(denied) => {
                if user
                    .selectors
                    .iter()
                    .any(|sel| Self::check_perms(sel, spec, args).is_ok())
                {
                    Ok(())
                } else {
                    Err(denied)
                }
            }
        }
    }

    fn check_perms(user: &AclUser, spec: &CommandSpec, args: &[Bytes]) -> Result<(), Denied> {
        if user.unrestricted {
            return Ok(());
        }
        if !user.cmd_allowed(spec.id) {
            let sub = args
                .first()
                .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase());
            let ok = sub.as_ref().is_some_and(|s| {
                user.allowed_sub
                    .iter()
                    .any(|(id, allowed)| *id == spec.id && allowed == s)
            });
            if !ok {
                let name = spec.name.to_lowercase();
                return Err(Denied::Command(match sub {
                    Some(s) if has_subcommands(spec.id) => format!("{}|{}", name, s),
                    _ => name,
                }));
            }
        }
        let all_keys = user
            .keys
            .iter()
            .any(|k| k.pattern == "*" && k.read && k.write);
        // Shard channels have key specs (for routing) but are channels.
        if !all_keys && !spec.has(F_PUBSUB) {
            let write = spec.is_write();
            for pos in spec.key_positions(args) {
                if let Some(k) = args.get(pos) {
                    if !user.key_allowed(k, write) {
                        return Err(Denied::Key(k.clone()));
                    }
                }
            }
        }
        if !user.all_channels {
            match spec.id {
                Cmd::PUBLISH | Cmd::SPUBLISH => {
                    if let Some(ch) = args.first() {
                        if !user.channel_allowed(ch, false) {
                            return Err(Denied::Channel(ch.clone()));
                        }
                    }
                }
                Cmd::SUBSCRIBE | Cmd::PSUBSCRIBE | Cmd::SSUBSCRIBE => {
                    let pat = spec.id == Cmd::PSUBSCRIBE;
                    for ch in args {
                        if !user.channel_allowed(ch, pat) {
                            return Err(Denied::Channel(ch.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Record a denial in `ACL LOG` (similar entries are counted together).
    pub fn log_denial(&self, user: &str, denied: &Denied, ctx: Context, client_info: String) {
        let (reason, object) = denied.reason();
        let now = now_ms();
        let mut log = self.log.lock();
        if let Some(e) = log.iter_mut().find(|e| {
            e.reason == reason
                && e.object == object
                && e.username == user
                && e.context == ctx.name()
        }) {
            e.count += 1;
            e.updated_ms = now;
            e.client_info = client_info;
            return;
        }
        log.push_front(LogEntry {
            id: self.log_id.fetch_add(1, Ordering::Relaxed),
            count: 1,
            reason,
            context: ctx.name(),
            object,
            username: user.to_string(),
            client_info,
            created_ms: now,
            updated_ms: now,
        });
        log.truncate(ACL_LOG_MAX);
    }

    pub fn log_auth_failure(&self, user: &str, client_info: String) {
        let now = now_ms();
        let mut log = self.log.lock();
        if let Some(e) = log
            .iter_mut()
            .find(|e| e.reason == "auth" && e.username == user)
        {
            e.count += 1;
            e.updated_ms = now;
            return;
        }
        log.push_front(LogEntry {
            id: self.log_id.fetch_add(1, Ordering::Relaxed),
            count: 1,
            reason: "auth",
            context: "toplevel",
            object: "AUTH".into(),
            username: user.to_string(),
            client_info,
            created_ms: now,
            updated_ms: now,
        });
        log.truncate(ACL_LOG_MAX);
    }

    pub fn set_file(&self, path: Option<std::path::PathBuf>) {
        *self.file.write() = path;
    }

    /// Replace every user with the ones in the ACL file.
    pub fn load_file(&self) -> Result<(), String> {
        let Some(path) = self.file.read().clone() else {
            return Err("ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.".into());
        };
        let text = std::fs::read_to_string(&path).map_err(|e| format!("ERR {}", e))?;
        let mut users = BTreeMap::new();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split_ascii_whitespace();
            if parts.next() != Some("user") {
                return Err(format!(
                    "ERR /{}:{}: should start with user keyword",
                    path.display(),
                    n + 1
                ));
            }
            let name = parts
                .next()
                .ok_or_else(|| format!("ERR {}:{}: missing user name", path.display(), n + 1))?;
            let mut u = AclUser::new(name);
            let rules = merge_selector_args(parts.map(str::to_string))
                .map_err(|e| format!("ERR {}:{}: {}", path.display(), n + 1, e))?;
            for rule in &rules {
                u.apply(rule)
                    .map_err(|e| format!("ERR Error in user declaration '{}': {}", rule, e))?;
            }
            users.insert(name.to_string(), Arc::new(u));
        }
        users
            .entry("default".to_string())
            .or_insert_with(|| Arc::new(AclUser::default_user(None)));
        *self.users.write() = users;
        self.bump();
        Ok(())
    }

    pub fn save_file(&self) -> Result<(), String> {
        let Some(path) = self.file.read().clone() else {
            return Err("ERR This Redis instance is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG REWRITE (assuming you have a Redis configuration file set) in order to store users in the Redis configuration.".into());
        };
        let mut out = String::new();
        for u in self.users.read().values() {
            out.push_str(&u.describe());
            out.push('\n');
        }
        solikv_persist::fsutil::write_atomic(&path, out).map_err(|e| format!("ERR {}", e))
    }

    /// `ACL <sub> …` (the connection-dependent parts — `WHOAMI`, the log's
    /// client info — come in through `whoami`).
    pub fn command(&self, args: &[Bytes], whoami: &str) -> CommandResponse {
        let sub = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        let rest = &args[1..];
        match sub.as_str() {
            "WHOAMI" => CommandResponse::bulk(Bytes::from(whoami.to_string())),
            "USERS" => CommandResponse::array(
                self.users
                    .read()
                    .keys()
                    .map(|n| CommandResponse::bulk(Bytes::from(n.clone())))
                    .collect(),
            ),
            "LIST" => CommandResponse::array(
                self.users
                    .read()
                    .values()
                    .map(|u| CommandResponse::bulk(Bytes::from(u.describe())))
                    .collect(),
            ),
            "SETUSER" => {
                let Some(name) = rest.first() else {
                    return CommandResponse::wrong_arity("acl|setuser");
                };
                let name = String::from_utf8_lossy(name).to_string();
                let mut u = self
                    .user(&name)
                    .map(|u| (*u).clone())
                    .unwrap_or_else(|| AclUser::new(&name));
                let rules = match merge_selector_args(
                    rest[1..]
                        .iter()
                        .map(|r| String::from_utf8_lossy(r).to_string()),
                ) {
                    Ok(r) => r,
                    Err(e) => return CommandResponse::error(format!("ERR {}", e)),
                };
                for rule in &rules {
                    if let Err(e) = u.apply(rule) {
                        return CommandResponse::error(format!(
                            "ERR Error in ACL SETUSER modifier '{}': {}",
                            rule, e
                        ));
                    }
                }
                self.users.write().insert(name, Arc::new(u));
                self.bump();
                CommandResponse::ok()
            }
            "GETUSER" => {
                let [name] = rest else {
                    return CommandResponse::wrong_arity("acl|getuser");
                };
                match self.user(&String::from_utf8_lossy(name)) {
                    Some(u) => u.getuser(),
                    None => CommandResponse::Nil,
                }
            }
            "DELUSER" => {
                let mut n = 0;
                let mut users = self.users.write();
                for name in rest {
                    let name = String::from_utf8_lossy(name);
                    if name == "default" {
                        return CommandResponse::error("ERR The 'default' user cannot be removed");
                    }
                    if users.remove(name.as_ref()).is_some() {
                        n += 1;
                    }
                }
                drop(users);
                self.bump();
                CommandResponse::integer(n)
            }
            "CAT" => match rest.first() {
                None => CommandResponse::array(
                    CATEGORIES
                        .iter()
                        .map(|c| CommandResponse::bulk_string(c))
                        .collect(),
                ),
                Some(c) => {
                    let c = String::from_utf8_lossy(c).to_ascii_lowercase();
                    let Some(bit) = cat_bit(&c) else {
                        return CommandResponse::error(format!("ERR Unknown category '{}'", c));
                    };
                    CommandResponse::array(
                        commands::all()
                            .iter()
                            .filter(|s| categories_of(s) & bit != 0)
                            .map(|s| CommandResponse::bulk(Bytes::from(s.name.to_lowercase())))
                            .collect(),
                    )
                }
            },
            "GENPASS" => {
                let bits = match rest.first() {
                    None => 256,
                    Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<usize>().ok()) {
                        Some(n) if (1..=4096).contains(&n) => n,
                        _ => {
                            return CommandResponse::error(
                                "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096",
                            )
                        }
                    },
                };
                let bytes = random_bytes(bits.div_ceil(8));
                let mut h = hex(&bytes);
                h.truncate(bits.div_ceil(4));
                CommandResponse::bulk(Bytes::from(h))
            }
            "LOG" => {
                if rest
                    .first()
                    .is_some_and(|a| a.eq_ignore_ascii_case(b"RESET"))
                {
                    self.log.lock().clear();
                    return CommandResponse::ok();
                }
                let count = match rest.first() {
                    None => 10,
                    Some(c) => match std::str::from_utf8(c)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(n) => n,
                        None => {
                            return CommandResponse::error(
                                "ERR value is out of range, must be positive",
                            )
                        }
                    },
                };
                let now = now_ms();
                let log = self.log.lock();
                CommandResponse::array(
                    log.iter()
                        .take(count)
                        .map(|e| {
                            CommandResponse::array(vec![
                                CommandResponse::bulk_string("count"),
                                CommandResponse::integer(e.count as i64),
                                CommandResponse::bulk_string("reason"),
                                CommandResponse::bulk_string(e.reason),
                                CommandResponse::bulk_string("context"),
                                CommandResponse::bulk_string(e.context),
                                CommandResponse::bulk_string("object"),
                                CommandResponse::bulk(Bytes::from(e.object.clone())),
                                CommandResponse::bulk_string("username"),
                                CommandResponse::bulk(Bytes::from(e.username.clone())),
                                CommandResponse::bulk_string("age-seconds"),
                                CommandResponse::bulk(Bytes::from(format!(
                                    "{:.3}",
                                    now.saturating_sub(e.updated_ms) as f64 / 1000.0
                                ))),
                                CommandResponse::bulk_string("client-info"),
                                CommandResponse::bulk(Bytes::from(e.client_info.clone())),
                                CommandResponse::bulk_string("entry-id"),
                                CommandResponse::integer(e.id as i64),
                                CommandResponse::bulk_string("timestamp-created"),
                                CommandResponse::integer(e.created_ms as i64),
                                CommandResponse::bulk_string("timestamp-last-updated"),
                                CommandResponse::integer(e.updated_ms as i64),
                            ])
                        })
                        .collect(),
                )
            }
            "DRYRUN" => {
                if rest.len() < 2 {
                    return CommandResponse::wrong_arity("acl|dryrun");
                }
                let name = String::from_utf8_lossy(&rest[0]).to_string();
                let Some(u) = self.user(&name) else {
                    return CommandResponse::error(format!("ERR User '{}' not found", name));
                };
                let cmd = String::from_utf8_lossy(&rest[1]).to_ascii_uppercase();
                let Some(spec) = commands::lookup(&cmd) else {
                    return CommandResponse::error(format!(
                        "ERR Command '{}' not found",
                        cmd.to_lowercase()
                    ));
                };
                if !spec.arity_ok(rest.len() - 2) {
                    return CommandResponse::error(format!(
                        "ERR wrong number of arguments for '{}' command",
                        cmd.to_lowercase()
                    ));
                }
                match self.check(&u, spec, &rest[2..]) {
                    Ok(()) => CommandResponse::ok(),
                    Err(Denied::Command(c)) => CommandResponse::bulk(Bytes::from(format!(
                        "User {} has no permissions to run the '{}' command",
                        name, c
                    ))),
                    Err(Denied::Key(k)) => CommandResponse::bulk(Bytes::from(format!(
                        "User {} has no permissions to access the '{}' key",
                        name,
                        String::from_utf8_lossy(&k)
                    ))),
                    Err(Denied::Channel(c)) => CommandResponse::bulk(Bytes::from(format!(
                        "User {} has no permissions to access the '{}' channel",
                        name,
                        String::from_utf8_lossy(&c)
                    ))),
                }
            }
            "SAVE" => match self.save_file() {
                Ok(()) => CommandResponse::ok(),
                Err(e) => CommandResponse::error(e),
            },
            "LOAD" => match self.load_file() {
                Ok(()) => CommandResponse::ok(),
                Err(e) => CommandResponse::error(e),
            },
            "HELP" => CommandResponse::array(
                [
                    "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "CAT [<category>]",
                    "DELUSER <username> [<username> ...]",
                    "DRYRUN <username> <command> [<arg> ...]",
                    "GETUSER <username>",
                    "LIST",
                    "USERS",
                    "SETUSER <username> <attribute> [<attribute> ...]",
                    "SAVE",
                    "LOAD",
                    "LOG [<count> | RESET]",
                    "GENPASS [<bits>]",
                    "WHOAMI",
                ]
                .iter()
                .map(|l| CommandResponse::simple(l))
                .collect(),
            ),
            _ => CommandResponse::error(format!(
                "ERR unknown subcommand '{}'. Try ACL HELP.",
                sub.to_lowercase()
            )),
        }
    }
}

/// Commands whose first argument is a subcommand (for `cmd|sub` rules and
/// messages).
fn has_subcommands(id: Cmd) -> bool {
    matches!(
        id,
        Cmd::CONFIG
            | Cmd::CLIENT
            | Cmd::SCRIPT
            | Cmd::FUNCTION
            | Cmd::OBJECT
            | Cmd::XINFO
            | Cmd::XGROUP
            | Cmd::PUBSUB
            | Cmd::COMMAND
            | Cmd::CLUSTER
            | Cmd::ACL
            | Cmd::SLOWLOG
            | Cmd::LATENCY
            | Cmd::MEMORY
            | Cmd::DEBUG
    )
}

fn random_bytes(n: usize) -> Vec<u8> {
    use std::io::Read;
    let mut buf = vec![0u8; n];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .is_err()
    {
        // No /dev/urandom: hash time and a counter (not for key material
        // an attacker can observe, but GENPASS output stays unpredictable
        // enough for a fallback).
        let seed = format!("{:?}{}", std::time::SystemTime::now(), n);
        let h = sha256(seed.as_bytes());
        for (i, b) in buf.iter_mut().enumerate() {
            *b = h[i % 32];
        }
    }
    buf
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn default_user_is_unrestricted() {
        let acl = Acl::default();
        assert!(acl.default_user().unrestricted());
        assert!(acl.default_auto_auth());
        acl.set_requirepass(Some("pw"));
        assert!(!acl.default_auto_auth());
        assert!(acl.authenticate(None, b"pw").is_ok());
        assert!(acl.authenticate(Some(b"default"), b"pw").is_ok());
        assert!(acl.authenticate(None, b"nope").is_err());
    }

    #[test]
    fn rules_restrict_commands_keys_and_channels() {
        let acl = Acl::default();
        let r = acl.command(
            &[
                b("SETUSER"),
                b("app"),
                b("on"),
                b(">secret"),
                b("~app:*"),
                b("%R~shared:*"),
                b("+@read"),
                b("+set"),
                b("&news"),
            ],
            "default",
        );
        assert!(matches!(r, CommandResponse::Ok), "{r:?}");
        let u = acl.authenticate(Some(b"app"), b"secret").ok().unwrap();
        assert!(!u.unrestricted());
        let get = commands::lookup("GET").unwrap();
        let set = commands::lookup("SET").unwrap();
        let del = commands::lookup("DEL").unwrap();
        let publish = commands::lookup("PUBLISH").unwrap();
        assert!(acl.check(&u, get, &[b("app:1")]).is_ok());
        assert!(acl.check(&u, get, &[b("shared:1")]).is_ok());
        assert!(matches!(
            acl.check(&u, set, &[b("shared:1"), b("v")]),
            Err(Denied::Key(_))
        ));
        assert!(acl.check(&u, set, &[b("app:1"), b("v")]).is_ok());
        assert!(matches!(
            acl.check(&u, del, &[b("app:1")]),
            Err(Denied::Command(_))
        ));
        assert!(acl.check(&u, publish, &[b("news"), b("x")]).is_err()); // no +publish
        acl.command(&[b("SETUSER"), b("app"), b("+publish")], "default");
        let u = acl.user("app").unwrap();
        assert!(acl.check(&u, publish, &[b("news"), b("x")]).is_ok());
        assert!(matches!(
            acl.check(&u, publish, &[b("other"), b("x")]),
            Err(Denied::Channel(_))
        ));
        let listed = acl.user("app").unwrap().describe();
        assert!(listed.starts_with("user app on #"), "{listed}");
        assert!(
            listed.contains("~app:*") && listed.contains("%R~shared:*"),
            "{listed}"
        );
    }

    #[test]
    fn subcommand_rules() {
        let acl = Acl::default();
        acl.command(
            &[
                b("SETUSER"),
                b("ops"),
                b("on"),
                b("nopass"),
                b("~*"),
                b("+config|get"),
            ],
            "default",
        );
        let u = acl.user("ops").unwrap();
        let config = commands::lookup("CONFIG").unwrap();
        assert!(acl.check(&u, config, &[b("get"), b("maxmemory")]).is_ok());
        match acl.check(&u, config, &[b("set"), b("maxmemory"), b("1")]) {
            Err(Denied::Command(c)) => assert_eq!(c, "config|set"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn categories_cover_the_basics() {
        let get = categories_of(commands::lookup("GET").unwrap());
        assert!(get & cat_bit("read").unwrap() != 0);
        assert!(get & cat_bit("string").unwrap() != 0);
        assert!(get & cat_bit("fast").unwrap() != 0);
        let flush = categories_of(commands::lookup("FLUSHALL").unwrap());
        assert!(flush & cat_bit("dangerous").unwrap() != 0);
        let hset = categories_of(commands::lookup("HSET").unwrap());
        assert!(hset & cat_bit("hash").unwrap() != 0);
        let hello = categories_of(commands::lookup("HELLO").unwrap());
        assert!(hello & cat_bit("hash").unwrap() == 0);
    }

    #[test]
    fn selectors_add_permission_sets() {
        let acl = Acl::default();
        // As a shell splits it: the selector arrives in two arguments.
        let r = acl.command(
            &[
                b("SETUSER"),
                b("sel"),
                b("on"),
                b("nopass"),
                b("~root:*"),
                b("+get"),
                b("(~sel:*"),
                b("+set)"),
                b("(%R~ro:* +get +strlen)"),
            ],
            "default",
        );
        assert!(matches!(r, CommandResponse::Ok), "{r:?}");
        let u = acl.user("sel").unwrap();
        let get = commands::lookup("GET").unwrap();
        let set = commands::lookup("SET").unwrap();
        let strlen = commands::lookup("STRLEN").unwrap();
        // Root: GET on root:*. First selector: SET on sel:*. Second: reads
        // of ro:*.
        assert!(acl.check(&u, get, &[b("root:1")]).is_ok());
        assert!(acl.check(&u, set, &[b("sel:1"), b("v")]).is_ok());
        assert!(acl.check(&u, strlen, &[b("ro:1")]).is_ok());
        assert!(acl.check(&u, get, &[b("ro:1")]).is_ok());
        // Each set is checked whole: no mixing a command from one with keys
        // from another.
        assert!(acl.check(&u, set, &[b("root:1"), b("v")]).is_err());
        assert!(acl.check(&u, get, &[b("sel:1")]).is_err());
        assert!(acl.check(&u, set, &[b("ro:1"), b("v")]).is_err());
        // The denial is the root permissions' one.
        assert!(matches!(
            acl.check(&u, set, &[b("other"), b("v")]),
            Err(Denied::Command(_))
        ));

        // ACL LIST writes them back, and that line loads to the same user.
        let line = u.describe();
        assert!(line.contains("(~sel:* resetchannels -@all +set)"), "{line}");
        let dir = std::env::temp_dir().join(format!("solikv-acl-sel-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("users.acl");
        std::fs::write(&file, format!("{line}\n")).unwrap();
        let acl2 = Acl::default();
        acl2.set_file(Some(file));
        acl2.load_file().unwrap();
        let u2 = acl2.user("sel").unwrap();
        assert_eq!(u2.describe(), line);
        assert!(acl2.check(&u2, set, &[b("sel:2"), b("v")]).is_ok());
        let _ = std::fs::remove_dir_all(&dir);

        // GETUSER lists them; clearselectors removes them.
        let CommandResponse::Array(fields) = u.getuser() else {
            panic!("GETUSER is an array");
        };
        assert!(matches!(&fields[fields.len() - 1], CommandResponse::Array(s) if s.len() == 2));
        acl.command(&[b("SETUSER"), b("sel"), b("clearselectors")], "default");
        let u = acl.user("sel").unwrap();
        assert!(acl.check(&u, set, &[b("sel:1"), b("v")]).is_err());

        // Selectors take permissions only; an open parenthesis is an error.
        for bad in [&["(on)"][..], &["(>pw)"], &["(~a", "+get"]] {
            let mut args = vec![b("SETUSER"), b("bad")];
            args.extend(bad.iter().map(|a| b(a)));
            let r = acl.command(&args, "default");
            assert!(matches!(r, CommandResponse::Error(_)), "{bad:?}: {r:?}");
        }
    }
}
