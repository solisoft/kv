//! Stream commands added for Redis 7 parity: `XCLAIM`, `XAUTOCLAIM`,
//! `XSETID`, `XGROUP SETID|CREATECONSUMER`, `XINFO GROUPS|CONSUMERS|STREAM
//! [FULL]`, `XPENDING … IDLE`, and the state readers the engine uses to log
//! consumer-group changes as effects.

use bytes::Bytes;

use crate::store::ShardStore;
use crate::stream_ops::format_single_entry;
use crate::types::*;

/// Options of `XCLAIM`.
#[derive(Debug, Clone, Default)]
pub struct XClaimOpts {
    /// Set the entries' idle time (ms) instead of resetting it.
    pub idle: Option<u64>,
    /// Set the delivery time (unix ms) instead of now.
    pub time: Option<u64>,
    pub retrycount: Option<u64>,
    pub force: bool,
    pub justid: bool,
    pub lastid: Option<StreamId>,
}

/// One claimed or deleted entry, for the engine's log records.
#[derive(Debug, Clone)]
pub struct Claimed {
    pub id: StreamId,
    /// `None`: the entry was gone from the stream and left the PEL.
    pub delivery: Option<(u64, u64)>,
}

fn id_bulk(id: StreamId) -> CommandResponse {
    CommandResponse::bulk(Bytes::from(id.to_string()))
}

fn nogroup(key: &Bytes, group: &Bytes, cmd: &str) -> CommandResponse {
    CommandResponse::error(format!(
        "NOGROUP No such key '{}' or consumer group '{}'{}",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group),
        if cmd.is_empty() {
            String::new()
        } else {
            format!(" in {} command", cmd)
        }
    ))
}

fn kv(name: &str, v: CommandResponse) -> [CommandResponse; 2] {
    [CommandResponse::bulk_string(name), v]
}

fn opt_int(v: Option<u64>) -> CommandResponse {
    match v {
        Some(n) => CommandResponse::integer(n as i64),
        None => CommandResponse::Nil,
    }
}

impl ShardStore {
    fn stream_mut(&mut self, key: &Bytes) -> Result<Option<&mut StreamValue>, CommandResponse> {
        match self.get(key).map(|e| &e.value) {
            None => return Ok(None),
            Some(RedisValue::Stream(_)) => {}
            Some(_) => return Err(CommandResponse::wrong_type()),
        }
        match self.get_mut(key).map(|e| &mut e.value) {
            Some(RedisValue::Stream(s)) => Ok(Some(s)),
            _ => Ok(None),
        }
    }

    fn stream_ref(&mut self, key: &Bytes) -> Result<Option<&StreamValue>, CommandResponse> {
        match self.get(key).map(|e| &e.value) {
            None => Ok(None),
            Some(RedisValue::Stream(s)) => Ok(Some(s)),
            Some(_) => Err(CommandResponse::wrong_type()),
        }
    }

    /// Claim `ids` (or, for `XAUTOCLAIM`, what the scan picks) for
    /// `consumer`. Returns the reply entries and what happened to each id.
    #[allow(clippy::too_many_arguments)]
    fn claim(
        sv: &mut StreamValue,
        group: &Bytes,
        consumer: &Bytes,
        min_idle: u64,
        ids: &[StreamId],
        opts: &XClaimOpts,
        now: u64,
        out: &mut Vec<CommandResponse>,
        effects: &mut Vec<Claimed>,
    ) -> Result<(), ()> {
        let StreamValue {
            entries, groups, ..
        } = sv;
        let grp = groups.get_mut(group).ok_or(())?;
        if let Some(last) = opts.lastid {
            if last > grp.last_delivered_id {
                grp.last_delivered_id = last;
            }
        }
        let delivery_time = match (opts.idle, opts.time) {
            (Some(idle), _) => now.saturating_sub(idle),
            (None, Some(t)) => t,
            (None, None) => now,
        };
        let c = grp
            .consumers
            .entry(consumer.clone())
            .or_insert_with(|| StreamConsumer::new(consumer.clone(), now));
        c.seen_time = now;
        for &id in ids {
            let in_pel = grp.pending.contains_key(&id);
            let exists = entries.contains_key(&id);
            if !exists {
                // Deleted from the stream: it leaves the PEL (Redis 7).
                if let Some(pe) = grp.pending.remove(&id) {
                    if let Some(owner) = grp.consumers.get_mut(&pe.consumer) {
                        owner.pending.remove(&id);
                    }
                    effects.push(Claimed { id, delivery: None });
                }
                continue;
            }
            if !in_pel {
                if !opts.force {
                    continue;
                }
                grp.pending.insert(
                    id,
                    PendingEntry {
                        id,
                        consumer: consumer.clone(),
                        delivery_time: now,
                        delivery_count: 0,
                    },
                );
                if let Some(c) = grp.consumers.get_mut(consumer) {
                    c.pending.insert(id, ());
                }
            } else {
                let pe = &grp.pending[&id];
                if min_idle > 0 && now.saturating_sub(pe.delivery_time) < min_idle {
                    continue;
                }
            }
            // Move ownership.
            let pe = grp.pending.get_mut(&id).expect("in PEL");
            if pe.consumer != *consumer {
                let prev = std::mem::replace(&mut pe.consumer, consumer.clone());
                if let Some(owner) = grp.consumers.get_mut(&prev) {
                    owner.pending.remove(&id);
                }
            }
            pe.delivery_time = delivery_time;
            if let Some(n) = opts.retrycount {
                pe.delivery_count = n;
            } else if !opts.justid {
                pe.delivery_count += 1;
            }
            let (t, n) = (pe.delivery_time, pe.delivery_count);
            if let Some(c) = grp.consumers.get_mut(consumer) {
                c.pending.insert(id, ());
                c.active_time = now;
            }
            effects.push(Claimed {
                id,
                delivery: Some((t, n)),
            });
            out.push(if opts.justid {
                id_bulk(id)
            } else {
                format_single_entry(&id, &entries[&id])
            });
        }
        Ok(())
    }

    /// `XCLAIM key group consumer min-idle-time id… [options]`.
    #[allow(clippy::too_many_arguments)]
    pub fn stream_xclaim(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        consumer: &Bytes,
        min_idle: u64,
        ids: &[StreamId],
        opts: &XClaimOpts,
        effects: &mut Vec<Claimed>,
    ) -> CommandResponse {
        let sv = match self.stream_mut(key) {
            Err(e) => return e,
            Ok(None) => return nogroup(key, group, "XCLAIM"),
            Ok(Some(sv)) => sv,
        };
        let mut out = Vec::new();
        let now = now_millis();
        if Self::claim(
            sv, group, consumer, min_idle, ids, opts, now, &mut out, effects,
        )
        .is_err()
        {
            return nogroup(key, group, "XCLAIM");
        }
        CommandResponse::array(out)
    }

    /// `XAUTOCLAIM key group consumer min-idle-time start [COUNT n]
    /// [JUSTID]`: `[next-start, claimed, deleted-ids]`.
    #[allow(clippy::too_many_arguments)]
    pub fn stream_xautoclaim(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        consumer: &Bytes,
        min_idle: u64,
        start: StreamId,
        count: usize,
        justid: bool,
        effects: &mut Vec<Claimed>,
    ) -> CommandResponse {
        let sv = match self.stream_mut(key) {
            Err(e) => return e,
            Ok(None) => return nogroup(key, group, "XAUTOCLAIM"),
            Ok(Some(sv)) => sv,
        };
        let now = now_millis();
        let Some(grp) = sv.groups.get(group) else {
            return nogroup(key, group, "XAUTOCLAIM");
        };
        // Redis scans at most count*10 PEL entries per call.
        let attempts = count.saturating_mul(10);
        let mut picked = Vec::new();
        let mut next = StreamId::MIN;
        for (scanned, (&id, pe)) in grp.pending.range(start..).enumerate() {
            if picked.len() >= count || scanned >= attempts {
                next = id;
                break;
            }
            let gone = !sv.entries.contains_key(&id);
            if gone || now.saturating_sub(pe.delivery_time) >= min_idle {
                picked.push(id);
            }
        }
        let opts = XClaimOpts {
            justid,
            ..XClaimOpts::default()
        };
        let mut out = Vec::new();
        let before = effects.len();
        let _ = Self::claim(
            sv, group, consumer, 0, &picked, &opts, now, &mut out, effects,
        );
        let deleted: Vec<CommandResponse> = effects[before..]
            .iter()
            .filter(|c| c.delivery.is_none())
            .map(|c| id_bulk(c.id))
            .collect();
        CommandResponse::array(vec![
            id_bulk(next),
            CommandResponse::array(out),
            CommandResponse::array(deleted),
        ])
    }

    /// `XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]`.
    pub fn stream_xsetid(
        &mut self,
        key: &Bytes,
        last_id: StreamId,
        entries_added: Option<u64>,
        max_deleted: Option<StreamId>,
    ) -> CommandResponse {
        let sv = match self.stream_mut(key) {
            Err(e) => return e,
            Ok(None) => return CommandResponse::error("ERR no such key"),
            Ok(Some(sv)) => sv,
        };
        if let Some(md) = max_deleted {
            if last_id < md {
                return CommandResponse::error(
                    "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                );
            }
        }
        if let Some(n) = entries_added {
            if (sv.len() as u64) > n {
                return CommandResponse::error(
                    "ERR The entries_added specified in XSETID is smaller than the target stream length",
                );
            }
        }
        if let Some((&top, _)) = sv.entries.last_key_value() {
            if last_id < top {
                return CommandResponse::error(
                    "ERR The ID specified in XSETID is smaller than the target stream top item",
                );
            }
        }
        sv.last_id = last_id;
        if let Some(n) = entries_added {
            sv.entries_added = n;
        }
        if let Some(md) = max_deleted {
            sv.max_deleted_id = md;
        }
        CommandResponse::ok()
    }

    /// `XGROUP SETID key group id|$ [ENTRIESREAD n]`.
    pub fn stream_xgroup_setid(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        id: Option<StreamId>,
        entries_read: Option<u64>,
    ) -> CommandResponse {
        let sv =
            match self.stream_mut(key) {
                Err(e) => return e,
                Ok(None) => return CommandResponse::error(
                    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE \
                     you may want to use the MKSTREAM option to create an empty stream \
                     automatically.",
                ),
                Ok(Some(sv)) => sv,
            };
        let (last, added) = (sv.last_id, sv.entries_added);
        let Some(g) = sv.groups.get_mut(group) else {
            return nogroup(key, group, "XGROUP SETID");
        };
        let id = id.unwrap_or(last);
        g.last_delivered_id = id;
        g.entries_read = match entries_read {
            Some(n) => Some(n),
            None if id >= last => Some(added),
            None => None,
        };
        CommandResponse::ok()
    }

    /// `XGROUP CREATECONSUMER key group consumer`: 1 if created.
    pub fn stream_xgroup_createconsumer(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        consumer: &Bytes,
    ) -> CommandResponse {
        let sv = match self.stream_mut(key) {
            Err(e) => return e,
            Ok(None) => return nogroup(key, group, "XGROUP CREATECONSUMER"),
            Ok(Some(sv)) => sv,
        };
        let Some(g) = sv.groups.get_mut(group) else {
            return nogroup(key, group, "XGROUP CREATECONSUMER");
        };
        if g.consumers.contains_key(consumer) {
            return CommandResponse::integer(0);
        }
        g.consumers.insert(
            consumer.clone(),
            StreamConsumer::new(consumer.clone(), now_millis()),
        );
        CommandResponse::integer(1)
    }

    /// A group's state for log records: last delivered id, entries read.
    pub fn stream_group_state(
        &mut self,
        key: &Bytes,
        group: &Bytes,
    ) -> Option<(StreamId, Option<u64>)> {
        let sv = self.stream_ref(key).ok()??;
        let g = sv.groups.get(group)?;
        Some((g.last_delivered_id, g.entries_read))
    }

    /// The PEL entries of `ids` (delivery time and count), for log records.
    pub fn stream_pel_of(&mut self, key: &Bytes, group: &Bytes, ids: &[StreamId]) -> Vec<Claimed> {
        let Ok(Some(sv)) = self.stream_ref(key) else {
            return Vec::new();
        };
        let Some(g) = sv.groups.get(group) else {
            return Vec::new();
        };
        ids.iter()
            .map(|&id| Claimed {
                id,
                delivery: g
                    .pending
                    .get(&id)
                    .map(|pe| (pe.delivery_time, pe.delivery_count)),
            })
            .collect()
    }

    fn lag(sv: &StreamValue, g: &ConsumerGroup) -> Option<u64> {
        if sv.entries_added == 0 {
            return Some(0);
        }
        // Deletions after what the group read make the count unknowable.
        if sv.max_deleted_id > g.last_delivered_id {
            return None;
        }
        g.entries_read.map(|r| sv.entries_added.saturating_sub(r))
    }

    fn group_info(sv: &StreamValue, g: &ConsumerGroup) -> CommandResponse {
        let mut v = Vec::with_capacity(12);
        v.extend(kv("name", CommandResponse::bulk(g.name.clone())));
        v.extend(kv(
            "consumers",
            CommandResponse::integer(g.consumers.len() as i64),
        ));
        v.extend(kv(
            "pending",
            CommandResponse::integer(g.pending.len() as i64),
        ));
        v.extend(kv("last-delivered-id", id_bulk(g.last_delivered_id)));
        v.extend(kv("entries-read", opt_int(g.entries_read)));
        v.extend(kv("lag", opt_int(Self::lag(sv, g))));
        CommandResponse::array(v)
    }

    /// `XINFO GROUPS key`.
    pub fn stream_xinfo_groups(&mut self, key: &Bytes) -> CommandResponse {
        let sv = match self.stream_ref(key) {
            Err(e) => return e,
            Ok(None) => return CommandResponse::error("ERR no such key"),
            Ok(Some(sv)) => sv,
        };
        let mut groups: Vec<&ConsumerGroup> = sv.groups.values().collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        CommandResponse::array(
            groups
                .into_iter()
                .map(|g| Self::group_info(sv, g))
                .collect(),
        )
    }

    /// `XINFO CONSUMERS key group`.
    pub fn stream_xinfo_consumers(&mut self, key: &Bytes, group: &Bytes) -> CommandResponse {
        let sv = match self.stream_ref(key) {
            Err(e) => return e,
            Ok(None) => return CommandResponse::error("ERR no such key"),
            Ok(Some(sv)) => sv,
        };
        let Some(g) = sv.groups.get(group) else {
            return nogroup(key, group, "XINFO CONSUMERS");
        };
        let now = now_millis();
        let mut cs: Vec<&StreamConsumer> = g.consumers.values().collect();
        cs.sort_by(|a, b| a.name.cmp(&b.name));
        CommandResponse::array(
            cs.into_iter()
                .map(|c| {
                    let mut v = Vec::with_capacity(8);
                    v.extend(kv("name", CommandResponse::bulk(c.name.clone())));
                    v.extend(kv(
                        "pending",
                        CommandResponse::integer(c.pending.len() as i64),
                    ));
                    v.extend(kv(
                        "idle",
                        CommandResponse::integer(now.saturating_sub(c.seen_time) as i64),
                    ));
                    v.extend(kv(
                        "inactive",
                        CommandResponse::integer(if c.active_time == 0 {
                            -1
                        } else {
                            now.saturating_sub(c.active_time) as i64
                        }),
                    ));
                    CommandResponse::array(v)
                })
                .collect(),
        )
    }

    /// `XINFO STREAM key [FULL [COUNT n]]` (`full = Some(count)`, 0 = all).
    pub fn stream_xinfo_full(&mut self, key: &Bytes, full: Option<usize>) -> CommandResponse {
        let sv = match self.stream_ref(key) {
            Err(e) => return e,
            Ok(None) => return CommandResponse::error("ERR no such key"),
            Ok(Some(sv)) => sv,
        };
        let first = sv.entries.first_key_value();
        let mut v = Vec::with_capacity(20);
        v.extend(kv("length", CommandResponse::integer(sv.len() as i64)));
        // No radix tree here; report the shape Redis would for these sizes.
        let nodes = sv.len().div_ceil(100) as i64;
        v.extend(kv("radix-tree-keys", CommandResponse::integer(nodes)));
        v.extend(kv("radix-tree-nodes", CommandResponse::integer(nodes + 1)));
        v.extend(kv("last-generated-id", id_bulk(sv.last_id)));
        v.extend(kv("max-deleted-entry-id", id_bulk(sv.max_deleted_id)));
        v.extend(kv(
            "entries-added",
            CommandResponse::integer(sv.entries_added as i64),
        ));
        v.extend(kv(
            "recorded-first-entry-id",
            id_bulk(first.map(|(id, _)| *id).unwrap_or(StreamId::MIN)),
        ));
        let Some(count) = full else {
            v.extend(kv(
                "groups",
                CommandResponse::integer(sv.groups.len() as i64),
            ));
            v.extend(kv(
                "first-entry",
                first
                    .map(|(id, f)| format_single_entry(id, f))
                    .unwrap_or(CommandResponse::Nil),
            ));
            v.extend(kv(
                "last-entry",
                sv.entries
                    .last_key_value()
                    .map(|(id, f)| format_single_entry(id, f))
                    .unwrap_or(CommandResponse::Nil),
            ));
            return CommandResponse::array(v);
        };
        let take = if count == 0 { usize::MAX } else { count };
        v.extend(kv(
            "entries",
            CommandResponse::array(
                sv.entries
                    .iter()
                    .take(take)
                    .map(|(id, f)| format_single_entry(id, f))
                    .collect(),
            ),
        ));
        let mut groups: Vec<&ConsumerGroup> = sv.groups.values().collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        let groups = groups
            .into_iter()
            .map(|g| {
                let mut gv = Vec::with_capacity(14);
                gv.extend(kv("name", CommandResponse::bulk(g.name.clone())));
                gv.extend(kv("last-delivered-id", id_bulk(g.last_delivered_id)));
                gv.extend(kv("entries-read", opt_int(g.entries_read)));
                gv.extend(kv("lag", opt_int(Self::lag(sv, g))));
                gv.extend(kv(
                    "pel-count",
                    CommandResponse::integer(g.pending.len() as i64),
                ));
                gv.extend(kv(
                    "pending",
                    CommandResponse::array(
                        g.pending
                            .values()
                            .take(take)
                            .map(|pe| {
                                CommandResponse::array(vec![
                                    id_bulk(pe.id),
                                    CommandResponse::bulk(pe.consumer.clone()),
                                    CommandResponse::integer(pe.delivery_time as i64),
                                    CommandResponse::integer(pe.delivery_count as i64),
                                ])
                            })
                            .collect(),
                    ),
                ));
                let mut cs: Vec<&StreamConsumer> = g.consumers.values().collect();
                cs.sort_by(|a, b| a.name.cmp(&b.name));
                gv.extend(kv(
                    "consumers",
                    CommandResponse::array(
                        cs.into_iter()
                            .map(|c| {
                                let mut cv = Vec::with_capacity(10);
                                cv.extend(kv("name", CommandResponse::bulk(c.name.clone())));
                                cv.extend(kv(
                                    "seen-time",
                                    CommandResponse::integer(c.seen_time as i64),
                                ));
                                cv.extend(kv(
                                    "active-time",
                                    CommandResponse::integer(if c.active_time == 0 {
                                        -1
                                    } else {
                                        c.active_time as i64
                                    }),
                                ));
                                cv.extend(kv(
                                    "pel-count",
                                    CommandResponse::integer(c.pending.len() as i64),
                                ));
                                cv.extend(kv(
                                    "pending",
                                    CommandResponse::array(
                                        c.pending
                                            .keys()
                                            .take(take)
                                            .filter_map(|id| g.pending.get(id))
                                            .map(|pe| {
                                                CommandResponse::array(vec![
                                                    id_bulk(pe.id),
                                                    CommandResponse::integer(
                                                        pe.delivery_time as i64,
                                                    ),
                                                    CommandResponse::integer(
                                                        pe.delivery_count as i64,
                                                    ),
                                                ])
                                            })
                                            .collect(),
                                    ),
                                ));
                                CommandResponse::array(cv)
                            })
                            .collect(),
                    ),
                ));
                CommandResponse::array(gv)
            })
            .collect();
        v.extend(kv("groups", CommandResponse::array(groups)));
        CommandResponse::array(v)
    }

    /// `XPENDING key group [[IDLE min] start end count [consumer]]`
    /// (extended form).
    #[allow(clippy::too_many_arguments)]
    pub fn stream_xpending_range(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        min_idle: Option<u64>,
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&Bytes>,
    ) -> CommandResponse {
        let sv = match self.stream_ref(key) {
            Err(e) => return e,
            Ok(None) => return nogroup(key, group, "XPENDING"),
            Ok(Some(sv)) => sv,
        };
        let Some(g) = sv.groups.get(group) else {
            return nogroup(key, group, "XPENDING");
        };
        let now = now_millis();
        CommandResponse::array(
            g.pending
                .range(start..=end)
                .map(|(_, pe)| pe)
                .filter(|pe| consumer.is_none_or(|c| pe.consumer == *c))
                .filter(|pe| min_idle.is_none_or(|m| now.saturating_sub(pe.delivery_time) >= m))
                .take(count)
                .map(|pe| {
                    CommandResponse::array(vec![
                        id_bulk(pe.id),
                        CommandResponse::bulk(pe.consumer.clone()),
                        CommandResponse::integer(now.saturating_sub(pe.delivery_time) as i64),
                        CommandResponse::integer(pe.delivery_count as i64),
                    ])
                })
                .collect(),
        )
    }
}
