use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

use crate::store::ShardStore;
use crate::types::*;

fn generate_stream_id(stream: &StreamValue, input: StreamIdInput) -> Result<StreamId, CommandResponse> {
    match input {
        StreamIdInput::Auto => {
            let ms = now_millis();
            let seq = if ms == stream.last_id.ms {
                stream.last_id.seq + 1
            } else if ms > stream.last_id.ms {
                0
            } else {
                // Clock went backwards, use last_id.ms
                stream.last_id.seq + 1
            };
            let ms = ms.max(stream.last_id.ms);
            Ok(StreamId::new(ms, seq))
        }
        StreamIdInput::Explicit(id) => {
            if id <= stream.last_id {
                Err(CommandResponse::error(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                ))
            } else {
                Ok(id)
            }
        }
        StreamIdInput::Partial(ms) => {
            let seq = if ms == stream.last_id.ms {
                stream.last_id.seq + 1
            } else if ms > stream.last_id.ms {
                0
            } else {
                return Err(CommandResponse::error(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                ));
            };
            Ok(StreamId::new(ms, seq))
        }
        StreamIdInput::Min | StreamIdInput::Max => {
            Err(CommandResponse::error("ERR Invalid stream ID specified as stream command argument"))
        }
    }
}

fn apply_trim(stream: &mut StreamValue, trim: &StreamTrim) -> i64 {
    let mut trimmed = 0i64;
    match trim {
        StreamTrim::MaxLen { threshold, .. } => {
            while stream.entries.len() > *threshold {
                if let Some((&id, _)) = stream.entries.iter().next() {
                    stream.entries.remove(&id);
                    trimmed += 1;
                } else {
                    break;
                }
            }
        }
        StreamTrim::MinId { threshold, .. } => {
            let to_remove: Vec<StreamId> = stream.entries
                .range(..(*threshold))
                .map(|(&id, _)| id)
                .collect();
            for id in to_remove {
                stream.entries.remove(&id);
                trimmed += 1;
            }
        }
    }
    trimmed
}

fn format_stream_entries(entries: &[(&StreamId, &Vec<(Bytes, Bytes)>)]) -> CommandResponse {
    let result: Vec<CommandResponse> = entries
        .iter()
        .map(|(id, fields)| format_single_entry(id, fields))
        .collect();
    CommandResponse::array(result)
}

fn format_single_entry(id: &StreamId, fields: &[(Bytes, Bytes)]) -> CommandResponse {
    let field_items: Vec<CommandResponse> = fields
        .iter()
        .flat_map(|(k, v)| vec![CommandResponse::bulk(k.clone()), CommandResponse::bulk(v.clone())])
        .collect();
    CommandResponse::array(vec![
        CommandResponse::bulk(Bytes::from(id.to_string())),
        CommandResponse::array(field_items),
    ])
}

impl ShardStore {
    fn get_or_create_stream(&mut self, key: &Bytes) -> Result<&mut StreamValue, CommandResponse> {
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::Stream(_)) {
                return Err(CommandResponse::wrong_type());
            }
        }

        if self.get(key).is_none() {
            self.set(key.clone(), RedisValue::Stream(StreamValue::new()), None);
        }

        match self.get_mut(key).unwrap() {
            KeyEntry { value: RedisValue::Stream(ref mut sv), .. } => Ok(sv),
            _ => unreachable!(),
        }
    }

    fn get_stream(&mut self, key: &Bytes) -> Result<Option<&StreamValue>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Stream(sv) => Ok(Some(sv)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    fn get_stream_mut(&mut self, key: &Bytes) -> Result<Option<&mut StreamValue>, CommandResponse> {
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::Stream(_)) {
                return Err(CommandResponse::wrong_type());
            }
        } else {
            return Ok(None);
        }
        match self.get_mut(key) {
            Some(KeyEntry { value: RedisValue::Stream(ref mut sv), .. }) => Ok(Some(sv)),
            _ => Ok(None),
        }
    }

    /// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] N] *|id field value ...
    pub fn stream_xadd(
        &mut self,
        key: &Bytes,
        id_input: StreamIdInput,
        fields: Vec<(Bytes, Bytes)>,
        nomkstream: bool,
        trim: Option<StreamTrim>,
    ) -> CommandResponse {
        // Check NOMKSTREAM: if key doesn't exist and nomkstream is set, return nil
        if nomkstream {
            match self.get_stream(key) {
                Err(e) => return e,
                Ok(None) => return CommandResponse::nil(),
                Ok(Some(_)) => {}
            }
        }

        let stream = match self.get_or_create_stream(key) {
            Ok(s) => s,
            Err(e) => return e,
        };

        let id = match generate_stream_id(stream, id_input) {
            Ok(id) => id,
            Err(e) => return e,
        };

        stream.entries.insert(id, fields);
        stream.last_id = id;

        if let Some(ref t) = trim {
            apply_trim(stream, t);
        }

        CommandResponse::bulk(Bytes::from(id.to_string()))
    }

    /// XLEN key
    pub fn stream_xlen(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_stream(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(sv)) => CommandResponse::integer(sv.len() as i64),
        }
    }

    /// XRANGE key start end [COUNT n]
    pub fn stream_xrange(
        &mut self,
        key: &Bytes,
        start: StreamId,
        end: StreamId,
        count: Option<usize>,
    ) -> CommandResponse {
        match self.get_stream(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(sv)) => {
                let iter = sv.entries.range(start..=end);
                let entries: Vec<_> = if let Some(c) = count {
                    iter.take(c).collect()
                } else {
                    iter.collect()
                };
                format_stream_entries(&entries)
            }
        }
    }

    /// XREVRANGE key end start [COUNT n]
    pub fn stream_xrevrange(
        &mut self,
        key: &Bytes,
        end: StreamId,
        start: StreamId,
        count: Option<usize>,
    ) -> CommandResponse {
        match self.get_stream(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(sv)) => {
                let iter = sv.entries.range(start..=end).rev();
                let entries: Vec<_> = if let Some(c) = count {
                    iter.take(c).collect()
                } else {
                    iter.collect()
                };
                format_stream_entries(&entries)
            }
        }
    }

    /// Per-shard helper for XREAD: returns entries after last_id for a single key.
    pub fn stream_xread_single(
        &mut self,
        key: &Bytes,
        last_id: StreamId,
        count: Option<usize>,
    ) -> CommandResponse {
        match self.get_stream(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(sv)) => {
                // Exclusive start: entries strictly after last_id
                let start = if last_id.seq < u64::MAX {
                    StreamId::new(last_id.ms, last_id.seq + 1)
                } else {
                    StreamId::new(last_id.ms.saturating_add(1), 0)
                };

                let iter = sv.entries.range(start..);
                let entries: Vec<_> = if let Some(c) = count {
                    iter.take(c).collect()
                } else {
                    iter.collect()
                };
                if entries.is_empty() {
                    return CommandResponse::nil();
                }
                format_stream_entries(&entries)
            }
        }
    }

    /// XDEL key id [id ...]
    pub fn stream_xdel(&mut self, key: &Bytes, ids: &[StreamId]) -> CommandResponse {
        match self.get_stream_mut(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(sv)) => {
                let mut deleted = 0i64;
                for id in ids {
                    if sv.entries.remove(id).is_some() {
                        deleted += 1;
                    }
                }
                // Note: unlike sets/lists, empty streams keep the key alive
                CommandResponse::integer(deleted)
            }
        }
    }

    /// XTRIM key MAXLEN|MINID [=|~] N
    pub fn stream_xtrim(&mut self, key: &Bytes, trim: StreamTrim) -> CommandResponse {
        match self.get_stream_mut(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(sv)) => {
                let trimmed = apply_trim(sv, &trim);
                CommandResponse::integer(trimmed)
            }
        }
    }

    /// XINFO STREAM key
    pub fn stream_xinfo(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_stream(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::error("ERR no such key"),
            Ok(Some(sv)) => {
                let first_entry = sv.entries.keys().next().copied().unwrap_or(StreamId::MIN);
                let last_entry = sv.entries.keys().next_back().copied().unwrap_or(StreamId::MIN);
                CommandResponse::array(vec![
                    CommandResponse::bulk_string("length"),
                    CommandResponse::integer(sv.len() as i64),
                    CommandResponse::bulk_string("first-entry"),
                    if sv.is_empty() {
                        CommandResponse::nil()
                    } else {
                        format_single_entry(&first_entry, sv.entries.get(&first_entry).unwrap())
                    },
                    CommandResponse::bulk_string("last-entry"),
                    if sv.is_empty() {
                        CommandResponse::nil()
                    } else {
                        format_single_entry(&last_entry, sv.entries.get(&last_entry).unwrap())
                    },
                    CommandResponse::bulk_string("groups"),
                    CommandResponse::integer(sv.groups.len() as i64),
                    CommandResponse::bulk_string("last-generated-id"),
                    CommandResponse::bulk(Bytes::from(sv.last_id.to_string())),
                ])
            }
        }
    }

    /// XGROUP CREATE key group id|$ [MKSTREAM]
    pub fn stream_xgroup_create(
        &mut self,
        key: &Bytes,
        group: Bytes,
        id: StreamIdInput,
        mkstream: bool,
    ) -> CommandResponse {
        // Check if stream exists
        match self.get_stream(key) {
            Err(e) => return e,
            Ok(None) => {
                if !mkstream {
                    return CommandResponse::error(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    );
                }
                // Create the stream
                self.set(key.clone(), RedisValue::Stream(StreamValue::new()), None);
            }
            Ok(Some(_)) => {}
        }

        let stream = match self.get_mut(key) {
            Some(KeyEntry { value: RedisValue::Stream(ref mut sv), .. }) => sv,
            _ => unreachable!(),
        };

        if stream.groups.contains_key(&group) {
            return CommandResponse::error("BUSYGROUP Consumer Group name already exists");
        }

        let last_delivered_id = match id {
            StreamIdInput::Auto | StreamIdInput::Max => stream.last_id,
            StreamIdInput::Min => StreamId::MIN,
            StreamIdInput::Explicit(sid) => sid,
            StreamIdInput::Partial(ms) => StreamId::new(ms, 0),
        };

        stream.groups.insert(
            group.clone(),
            ConsumerGroup {
                name: group,
                last_delivered_id,
                pending: BTreeMap::new(),
                consumers: HashMap::new(),
            },
        );

        CommandResponse::ok()
    }

    /// XGROUP DESTROY key group
    pub fn stream_xgroup_destroy(&mut self, key: &Bytes, group: &Bytes) -> CommandResponse {
        match self.get_stream_mut(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(sv)) => {
                if sv.groups.remove(group).is_some() {
                    CommandResponse::integer(1)
                } else {
                    CommandResponse::integer(0)
                }
            }
        }
    }

    /// XGROUP DELCONSUMER key group consumer
    pub fn stream_xgroup_delconsumer(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        consumer: &Bytes,
    ) -> CommandResponse {
        match self.get_stream_mut(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(sv)) => {
                let grp = match sv.groups.get_mut(group) {
                    Some(g) => g,
                    None => return CommandResponse::error("NOGROUP No such consumer group for key"),
                };
                match grp.consumers.remove(consumer) {
                    Some(c) => {
                        let count = c.pending.len() as i64;
                        // Remove consumer's pending entries from group PEL
                        for (id, _) in &c.pending {
                            grp.pending.remove(id);
                        }
                        CommandResponse::integer(count)
                    }
                    None => CommandResponse::integer(0),
                }
            }
        }
    }

    /// Per-shard helper for XREADGROUP.
    pub fn stream_xreadgroup_single(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        consumer_name: &Bytes,
        id_input: StreamIdInput,
        count: Option<usize>,
    ) -> CommandResponse {
        match self.get_stream_mut(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::error("ERR The XREADGROUP subcommand requires the key to exist"),
            Ok(Some(sv)) => {
                let grp = match sv.groups.get_mut(group) {
                    Some(g) => g,
                    None => return CommandResponse::error("NOGROUP No such consumer group for key"),
                };

                // Ensure consumer exists
                if !grp.consumers.contains_key(consumer_name) {
                    grp.consumers.insert(
                        consumer_name.clone(),
                        StreamConsumer {
                            name: consumer_name.clone(),
                            seen_time: now_millis(),
                            pending: BTreeMap::new(),
                        },
                    );
                }

                match id_input {
                    // ">" means: deliver new entries not yet delivered to this group
                    StreamIdInput::Max => {
                        let start = if grp.last_delivered_id.seq < u64::MAX {
                            StreamId::new(
                                grp.last_delivered_id.ms,
                                grp.last_delivered_id.seq + 1,
                            )
                        } else {
                            StreamId::new(grp.last_delivered_id.ms.saturating_add(1), 0)
                        };

                        let entries_data: Vec<(StreamId, Vec<(Bytes, Bytes)>)> = {
                            let iter = sv.entries.range(start..);
                            if let Some(c) = count {
                                iter.take(c).map(|(&id, fields)| (id, fields.clone())).collect()
                            } else {
                                iter.map(|(&id, fields)| (id, fields.clone())).collect()
                            }
                        };

                        if entries_data.is_empty() {
                            return CommandResponse::nil();
                        }

                        let now = now_millis();
                        let last_delivered = entries_data.last().unwrap().0;
                        grp.last_delivered_id = last_delivered;

                        for (id, _) in &entries_data {
                            grp.pending.insert(*id, PendingEntry {
                                id: *id,
                                consumer: consumer_name.clone(),
                                delivery_time: now,
                                delivery_count: 1,
                            });
                            let consumer = grp.consumers.get_mut(consumer_name).unwrap();
                            consumer.pending.insert(*id, ());
                            consumer.seen_time = now;
                        }

                        let result: Vec<CommandResponse> = entries_data.iter().map(|(id, fields)| {
                            format_single_entry(id, fields)
                        }).collect();

                        CommandResponse::array(result)
                    }
                    // Specific ID means: re-deliver pending entries for this consumer
                    _ => {
                        let start_id = match id_input {
                            StreamIdInput::Min => StreamId::MIN,
                            StreamIdInput::Explicit(id) => id,
                            StreamIdInput::Partial(ms) => StreamId::new(ms, 0),
                            _ => StreamId::MIN,
                        };

                        let consumer = match grp.consumers.get_mut(consumer_name) {
                            Some(c) => c,
                            None => return CommandResponse::array(vec![]),
                        };

                        let pending_ids: Vec<StreamId> = consumer.pending
                            .range(start_id..)
                            .map(|(&id, _)| id)
                            .collect();

                        let pending_ids = if let Some(c) = count {
                            pending_ids.into_iter().take(c).collect::<Vec<_>>()
                        } else {
                            pending_ids
                        };

                        let now = now_millis();
                        consumer.seen_time = now;

                        let mut result = Vec::new();
                        for id in pending_ids {
                            if let Some(fields) = sv.entries.get(&id) {
                                // Update delivery info
                                if let Some(pe) = grp.pending.get_mut(&id) {
                                    pe.delivery_time = now;
                                    pe.delivery_count += 1;
                                }
                                result.push(format_single_entry(&id, fields));
                            }
                        }

                        CommandResponse::array(result)
                    }
                }
            }
        }
    }

    /// XACK key group id [id ...]
    pub fn stream_xack(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        ids: &[StreamId],
    ) -> CommandResponse {
        match self.get_stream_mut(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(sv)) => {
                let grp = match sv.groups.get_mut(group) {
                    Some(g) => g,
                    None => return CommandResponse::integer(0),
                };

                let mut acked = 0i64;
                for id in ids {
                    if let Some(pe) = grp.pending.remove(id) {
                        if let Some(consumer) = grp.consumers.get_mut(&pe.consumer) {
                            consumer.pending.remove(id);
                        }
                        acked += 1;
                    }
                }

                CommandResponse::integer(acked)
            }
        }
    }

    /// XPENDING key group [start end count [consumer]]
    pub fn stream_xpending(
        &mut self,
        key: &Bytes,
        group: &Bytes,
        start: Option<StreamId>,
        end: Option<StreamId>,
        count: Option<usize>,
        consumer_filter: Option<&Bytes>,
    ) -> CommandResponse {
        match self.get_stream(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::error("ERR no such key"),
            Ok(Some(sv)) => {
                let grp = match sv.groups.get(group) {
                    Some(g) => g,
                    None => return CommandResponse::error("NOGROUP No such consumer group for key"),
                };

                // Summary form: no start/end/count
                if start.is_none() && end.is_none() && count.is_none() {
                    let total = grp.pending.len() as i64;
                    let min_id = grp.pending.keys().next().copied();
                    let max_id = grp.pending.keys().next_back().copied();

                    // Count per consumer
                    let mut consumer_counts: Vec<CommandResponse> = Vec::new();
                    for (name, consumer) in &grp.consumers {
                        if !consumer.pending.is_empty() {
                            consumer_counts.push(CommandResponse::array(vec![
                                CommandResponse::bulk(name.clone()),
                                CommandResponse::bulk(Bytes::from(consumer.pending.len().to_string())),
                            ]));
                        }
                    }

                    return CommandResponse::array(vec![
                        CommandResponse::integer(total),
                        match min_id {
                            Some(id) => CommandResponse::bulk(Bytes::from(id.to_string())),
                            None => CommandResponse::nil(),
                        },
                        match max_id {
                            Some(id) => CommandResponse::bulk(Bytes::from(id.to_string())),
                            None => CommandResponse::nil(),
                        },
                        CommandResponse::array(consumer_counts),
                    ]);
                }

                // Detail form
                let start = start.unwrap_or(StreamId::MIN);
                let end = end.unwrap_or(StreamId::MAX);
                let count = count.unwrap_or(10);

                let iter = grp.pending.range(start..=end);
                let mut result = Vec::new();

                for (_, pe) in iter.take(count) {
                    if let Some(filter) = consumer_filter {
                        if &pe.consumer != filter {
                            continue;
                        }
                    }
                    result.push(CommandResponse::array(vec![
                        CommandResponse::bulk(Bytes::from(pe.id.to_string())),
                        CommandResponse::bulk(pe.consumer.clone()),
                        CommandResponse::integer(
                            now_millis().saturating_sub(pe.delivery_time) as i64,
                        ),
                        CommandResponse::integer(pe.delivery_count as i64),
                    ]));
                }

                CommandResponse::array(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- StreamId tests ----

    #[test]
    fn test_stream_id_ordering() {
        let a = StreamId::new(100, 0);
        let b = StreamId::new(100, 1);
        let c = StreamId::new(200, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
        assert_eq!(StreamId::MIN, StreamId::new(0, 0));
    }

    #[test]
    fn test_stream_id_display() {
        assert_eq!(StreamId::new(1000, 5).to_string(), "1000-5");
        assert_eq!(StreamId::MIN.to_string(), "0-0");
    }

    #[test]
    fn test_stream_id_parse() {
        assert_eq!(StreamId::parse("*"), Some(StreamIdInput::Auto));
        assert_eq!(StreamId::parse("-"), Some(StreamIdInput::Min));
        assert_eq!(StreamId::parse("+"), Some(StreamIdInput::Max));
        assert_eq!(StreamId::parse("100-5"), Some(StreamIdInput::Explicit(StreamId::new(100, 5))));
        assert_eq!(StreamId::parse("100"), Some(StreamIdInput::Partial(100)));
        assert_eq!(StreamId::parse("abc"), None);
        assert_eq!(StreamId::parse("100-abc"), None);
    }

    // ---- XADD + XLEN ----

    #[test]
    fn test_xadd_auto_id() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        let r = store.stream_xadd(
            &key,
            StreamIdInput::Auto,
            vec![(Bytes::from("name"), Bytes::from("Sara"))],
            false,
            None,
        );
        match r {
            CommandResponse::BulkString(id) => {
                let s = std::str::from_utf8(&id).unwrap();
                assert!(s.contains('-'));
            }
            _ => panic!("expected bulk string, got {:?}", r),
        }
        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(1)));
    }

    #[test]
    fn test_xadd_explicit_id() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        let r = store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 0)),
            vec![(Bytes::from("a"), Bytes::from("1"))],
            false,
            None,
        );
        match r {
            CommandResponse::BulkString(id) => assert_eq!(id, Bytes::from("1000-0")),
            _ => panic!("expected bulk string"),
        }

        let r = store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 1)),
            vec![(Bytes::from("b"), Bytes::from("2"))],
            false,
            None,
        );
        match r {
            CommandResponse::BulkString(id) => assert_eq!(id, Bytes::from("1000-1")),
            _ => panic!("expected bulk string"),
        }

        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(2)));
    }

    #[test]
    fn test_xadd_reject_lower_id() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 5)),
            vec![(Bytes::from("a"), Bytes::from("1"))],
            false,
            None,
        );
        let r = store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 3)),
            vec![(Bytes::from("b"), Bytes::from("2"))],
            false,
            None,
        );
        assert!(r.is_error());
    }

    #[test]
    fn test_xadd_nomkstream() {
        let mut store = ShardStore::new();
        let key = Bytes::from("noexist");
        let r = store.stream_xadd(
            &key,
            StreamIdInput::Auto,
            vec![(Bytes::from("a"), Bytes::from("1"))],
            true,
            None,
        );
        assert!(matches!(r, CommandResponse::Nil));
    }

    // ---- XRANGE / XREVRANGE ----

    #[test]
    fn test_xrange() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=5 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        match store.stream_xrange(&key, StreamId::MIN, StreamId::MAX, None) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 5),
            r => panic!("expected array, got {:?}", r),
        }

        match store.stream_xrange(&key, StreamId::new(2000, 0), StreamId::new(4000, 0), None) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 3),
            r => panic!("expected array, got {:?}", r),
        }

        match store.stream_xrange(&key, StreamId::MIN, StreamId::MAX, Some(2)) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            r => panic!("expected array, got {:?}", r),
        }
    }

    #[test]
    fn test_xrevrange() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=5 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        match store.stream_xrevrange(&key, StreamId::MAX, StreamId::MIN, Some(2)) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    CommandResponse::Array(inner) => {
                        match &inner[0] {
                            CommandResponse::BulkString(id) => assert_eq!(id, &Bytes::from("5000-0")),
                            _ => panic!("expected id"),
                        }
                    }
                    _ => panic!("expected array"),
                }
            }
            r => panic!("expected array, got {:?}", r),
        }
    }

    // ---- XDEL ----

    #[test]
    fn test_xdel() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 0)),
            vec![(Bytes::from("a"), Bytes::from("1"))],
            false,
            None,
        );
        store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(2000, 0)),
            vec![(Bytes::from("b"), Bytes::from("2"))],
            false,
            None,
        );

        let r = store.stream_xdel(&key, &[StreamId::new(1000, 0)]);
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(1)));

        let r = store.stream_xdel(&key, &[StreamId::new(9999, 0)]);
        assert!(matches!(r, CommandResponse::Integer(0)));
    }

    // ---- XTRIM ----

    #[test]
    fn test_xtrim_maxlen() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=10 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        let r = store.stream_xtrim(&key, StreamTrim::MaxLen { exact: true, threshold: 5 });
        assert!(matches!(r, CommandResponse::Integer(5)));
        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(5)));
    }

    #[test]
    fn test_xtrim_minid() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=10 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        let r = store.stream_xtrim(
            &key,
            StreamTrim::MinId { exact: true, threshold: StreamId::new(6000, 0) },
        );
        assert!(matches!(r, CommandResponse::Integer(5)));
        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(5)));
    }

    #[test]
    fn test_xadd_with_maxlen() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=10 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                Some(StreamTrim::MaxLen { exact: true, threshold: 5 }),
            );
        }
        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(5)));
    }

    // ---- XREAD single ----

    #[test]
    fn test_xread_single() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=5 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        match store.stream_xread_single(&key, StreamId::MIN, None) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 5),
            r => panic!("expected array, got {:?}", r),
        }

        match store.stream_xread_single(&key, StreamId::new(3000, 0), Some(10)) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            r => panic!("expected array, got {:?}", r),
        }

        match store.stream_xread_single(&key, StreamId::new(5000, 0), None) {
            CommandResponse::Nil => {}
            r => panic!("expected nil, got {:?}", r),
        }
    }

    // ---- Consumer groups ----

    #[test]
    fn test_xgroup_create_destroy() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 0)),
            vec![(Bytes::from("a"), Bytes::from("1"))],
            false,
            None,
        );

        let r = store.stream_xgroup_create(&key, Bytes::from("grp1"), StreamIdInput::Min, false);
        assert!(matches!(r, CommandResponse::Ok));

        let r = store.stream_xgroup_create(&key, Bytes::from("grp1"), StreamIdInput::Min, false);
        assert!(r.is_error());

        let r = store.stream_xgroup_destroy(&key, &Bytes::from("grp1"));
        assert!(matches!(r, CommandResponse::Integer(1)));

        let r = store.stream_xgroup_destroy(&key, &Bytes::from("grp1"));
        assert!(matches!(r, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_xgroup_create_mkstream() {
        let mut store = ShardStore::new();
        let key = Bytes::from("noexist");

        let r = store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);
        assert!(r.is_error());

        let r = store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, true);
        assert!(matches!(r, CommandResponse::Ok));
        assert!(matches!(store.stream_xlen(&key), CommandResponse::Integer(0)));
    }

    #[test]
    fn test_xreadgroup_new_entries() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=3 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);

        let r = store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("consumer1"),
            StreamIdInput::Max,
            Some(2),
        );
        match r {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            r => panic!("expected array, got {:?}", r),
        }

        let r = store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("consumer1"),
            StreamIdInput::Max,
            None,
        );
        match r {
            CommandResponse::Array(items) => assert_eq!(items.len(), 1),
            r => panic!("expected array, got {:?}", r),
        }

        let r = store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("consumer1"),
            StreamIdInput::Max,
            None,
        );
        assert!(matches!(r, CommandResponse::Nil));
    }

    #[test]
    fn test_xreadgroup_pending_redelivery() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=3 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);

        store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Max,
            None,
        );

        let r = store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Min,
            None,
        );
        match r {
            CommandResponse::Array(items) => assert_eq!(items.len(), 3),
            r => panic!("expected 3 pending entries, got {:?}", r),
        }
    }

    // ---- XACK ----

    #[test]
    fn test_xack() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=3 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);
        store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Max,
            None,
        );

        let r = store.stream_xack(
            &key,
            &Bytes::from("grp"),
            &[StreamId::new(1000, 0), StreamId::new(2000, 0)],
        );
        assert!(matches!(r, CommandResponse::Integer(2)));

        let r = store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Min,
            None,
        );
        match r {
            CommandResponse::Array(items) => assert_eq!(items.len(), 1),
            r => panic!("expected 1 pending, got {:?}", r),
        }
    }

    // ---- XPENDING ----

    #[test]
    fn test_xpending_summary() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=3 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);
        store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Max,
            None,
        );

        let r = store.stream_xpending(&key, &Bytes::from("grp"), None, None, None, None);
        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 4);
                match &items[0] {
                    CommandResponse::Integer(n) => assert_eq!(*n, 3),
                    _ => panic!("expected integer count"),
                }
            }
            r => panic!("expected array, got {:?}", r),
        }
    }

    #[test]
    fn test_xpending_detail() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=3 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);
        store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Max,
            None,
        );

        let r = store.stream_xpending(
            &key,
            &Bytes::from("grp"),
            Some(StreamId::MIN),
            Some(StreamId::MAX),
            Some(10),
            None,
        );
        match r {
            CommandResponse::Array(items) => assert_eq!(items.len(), 3),
            r => panic!("expected 3 detail entries, got {:?}", r),
        }
    }

    // ---- XINFO ----

    #[test]
    fn test_xinfo() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        store.stream_xadd(
            &key,
            StreamIdInput::Explicit(StreamId::new(1000, 0)),
            vec![(Bytes::from("a"), Bytes::from("1"))],
            false,
            None,
        );

        match store.stream_xinfo(&key) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 10); // 5 pairs
            }
            r => panic!("expected array, got {:?}", r),
        }
    }

    // ---- WRONGTYPE ----

    #[test]
    fn test_wrongtype() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mystr");
        store.set(key.clone(), RedisValue::String(Bytes::from("hello")), None);

        let r = store.stream_xadd(
            &key,
            StreamIdInput::Auto,
            vec![(Bytes::from("a"), Bytes::from("1"))],
            false,
            None,
        );
        assert!(r.is_error());

        let r = store.stream_xlen(&key);
        assert!(r.is_error());
    }

    // ---- XGROUP DELCONSUMER ----

    #[test]
    fn test_xgroup_delconsumer() {
        let mut store = ShardStore::new();
        let key = Bytes::from("stream");
        for i in 1..=3 {
            store.stream_xadd(
                &key,
                StreamIdInput::Explicit(StreamId::new(i * 1000, 0)),
                vec![(Bytes::from("n"), Bytes::from(i.to_string()))],
                false,
                None,
            );
        }

        store.stream_xgroup_create(&key, Bytes::from("grp"), StreamIdInput::Min, false);
        store.stream_xreadgroup_single(
            &key,
            &Bytes::from("grp"),
            &Bytes::from("c1"),
            StreamIdInput::Max,
            None,
        );

        let r = store.stream_xgroup_delconsumer(&key, &Bytes::from("grp"), &Bytes::from("c1"));
        assert!(matches!(r, CommandResponse::Integer(3)));
    }

    // ---- Empty stream / nonexistent key ----

    #[test]
    fn test_xlen_nonexistent() {
        let mut store = ShardStore::new();
        assert!(matches!(store.stream_xlen(&Bytes::from("nope")), CommandResponse::Integer(0)));
    }

    #[test]
    fn test_xrange_nonexistent() {
        let mut store = ShardStore::new();
        match store.stream_xrange(&Bytes::from("nope"), StreamId::MIN, StreamId::MAX, None) {
            CommandResponse::Array(items) => assert!(items.is_empty()),
            r => panic!("expected empty array, got {:?}", r),
        }
    }
}
