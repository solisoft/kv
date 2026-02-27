use bytes::Bytes;
use std::collections::VecDeque;

use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_list(&mut self, key: &Bytes) -> Result<&mut VecDeque<Bytes>, CommandResponse> {
        // Check type first
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::List(_)) {
                return Err(CommandResponse::wrong_type());
            }
        }

        if self.get(key).is_none() {
            self.set(key.clone(), RedisValue::List(VecDeque::new()), None);
        }

        match self.get_mut(key).unwrap() {
            KeyEntry { value: RedisValue::List(ref mut list), .. } => Ok(list),
            _ => unreachable!(),
        }
    }

    fn get_list(&mut self, key: &Bytes) -> Result<Option<&VecDeque<Bytes>>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::List(list) => Ok(Some(list)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    pub fn list_lpush(&mut self, key: &Bytes, values: Vec<Bytes>) -> CommandResponse {
        let list = match self.get_or_create_list(key) {
            Ok(l) => l,
            Err(e) => return e,
        };
        for v in values {
            list.push_front(v);
        }
        CommandResponse::integer(list.len() as i64)
    }

    pub fn list_rpush(&mut self, key: &Bytes, values: Vec<Bytes>) -> CommandResponse {
        let list = match self.get_or_create_list(key) {
            Ok(l) => l,
            Err(e) => return e,
        };
        for v in values {
            list.push_back(v);
        }
        CommandResponse::integer(list.len() as i64)
    }

    pub fn list_lpop(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::nil(),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::List(list) => {
                if count == 1 {
                    match list.pop_front() {
                        Some(v) => {
                            let resp = CommandResponse::bulk(v);
                            if list.is_empty() {
                                self.del(key);
                            }
                            resp
                        }
                        None => CommandResponse::nil(),
                    }
                } else {
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        match list.pop_front() {
                            Some(v) => items.push(CommandResponse::bulk(v)),
                            None => break,
                        }
                    }
                    if list.is_empty() {
                        self.del(key);
                    }
                    if items.is_empty() {
                        CommandResponse::nil()
                    } else {
                        CommandResponse::array(items)
                    }
                }
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn list_rpop(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::nil(),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::List(list) => {
                if count == 1 {
                    match list.pop_back() {
                        Some(v) => {
                            let resp = CommandResponse::bulk(v);
                            if list.is_empty() {
                                self.del(key);
                            }
                            resp
                        }
                        None => CommandResponse::nil(),
                    }
                } else {
                    let mut items = Vec::with_capacity(count);
                    for _ in 0..count {
                        match list.pop_back() {
                            Some(v) => items.push(CommandResponse::bulk(v)),
                            None => break,
                        }
                    }
                    if list.is_empty() {
                        self.del(key);
                    }
                    if items.is_empty() {
                        CommandResponse::nil()
                    } else {
                        CommandResponse::array(items)
                    }
                }
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn list_llen(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_list(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(list)) => CommandResponse::integer(list.len() as i64),
        }
    }

    pub fn list_lindex(&mut self, key: &Bytes, index: i64) -> CommandResponse {
        match self.get_list(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::nil(),
            Ok(Some(list)) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    CommandResponse::nil()
                } else {
                    CommandResponse::bulk(list[idx as usize].clone())
                }
            }
        }
    }

    pub fn list_lset(&mut self, key: &Bytes, index: i64, value: Bytes) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::error("ERR no such key"),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::List(list) => {
                let len = list.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    CommandResponse::error("ERR index out of range")
                } else {
                    list[idx as usize] = value;
                    CommandResponse::ok()
                }
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn list_lrange(&mut self, key: &Bytes, start: i64, stop: i64) -> CommandResponse {
        match self.get_list(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(list)) => {
                let len = list.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start };
                let e = if stop < 0 { len + stop } else { stop.min(len - 1) };
                if s > e || s >= len {
                    return CommandResponse::array(vec![]);
                }
                let items: Vec<CommandResponse> = (s..=e)
                    .filter_map(|i| list.get(i as usize).map(|v| CommandResponse::bulk(v.clone())))
                    .collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn list_linsert(&mut self, key: &Bytes, before: bool, pivot: &Bytes, value: Bytes) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::List(list) => {
                if let Some(pos) = list.iter().position(|v| v == pivot) {
                    let idx = if before { pos } else { pos + 1 };
                    list.insert(idx, value);
                    CommandResponse::integer(list.len() as i64)
                } else {
                    CommandResponse::integer(-1)
                }
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn list_lrem(&mut self, key: &Bytes, count: i64, value: &Bytes) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::List(list) => {
                let mut removed = 0i64;
                if count > 0 {
                    let mut i = 0;
                    while i < list.len() && removed < count {
                        if &list[i] == value {
                            list.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else if count < 0 {
                    let limit = count.unsigned_abs() as i64;
                    let mut i = list.len();
                    while i > 0 && removed < limit {
                        i -= 1;
                        if &list[i] == value {
                            list.remove(i);
                            removed += 1;
                        }
                    }
                } else {
                    list.retain(|v| {
                        if v == value {
                            removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                }
                if list.is_empty() {
                    self.del(key);
                }
                CommandResponse::integer(removed)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn list_ltrim(&mut self, key: &Bytes, start: i64, stop: i64) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::ok(),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::List(list) => {
                let len = list.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start };
                let e = if stop < 0 { len + stop } else { stop.min(len - 1) };
                if s > e || s >= len {
                    list.clear();
                } else {
                    let new_list: VecDeque<Bytes> = list
                        .iter()
                        .skip(s as usize)
                        .take((e - s + 1) as usize)
                        .cloned()
                        .collect();
                    *list = new_list;
                }
                if list.is_empty() {
                    self.del(key);
                }
                CommandResponse::ok()
            }
            _ => CommandResponse::wrong_type(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lpush_rpush() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_lpush(&key, vec![Bytes::from("b"), Bytes::from("a")]);
        store.list_rpush(&key, vec![Bytes::from("c")]);
        // List should be: a, b, c
        match store.list_lrange(&key, 0, -1) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_lpop_rpop() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);

        match store.list_lpop(&key, 1) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("a")),
            _ => panic!("expected bulk"),
        }
        match store.list_rpop(&key, 1) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("c")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_llen() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        assert!(matches!(store.list_llen(&key), CommandResponse::Integer(0)));
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        assert!(matches!(store.list_llen(&key), CommandResponse::Integer(2)));
    }

    #[test]
    fn test_lindex() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        match store.list_lindex(&key, 0) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("a")),
            _ => panic!("expected bulk"),
        }
        match store.list_lindex(&key, -1) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("c")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_lset() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        store.list_lset(&key, 1, Bytes::from("B"));
        match store.list_lindex(&key, 1) {
            CommandResponse::BulkString(v) => assert_eq!(v, Bytes::from("B")),
            _ => panic!("expected bulk"),
        }
    }

    #[test]
    fn test_lrange() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c"), Bytes::from("d")]);
        match store.list_lrange(&key, 1, 2) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("b")));
                assert!(matches!(&items[1], CommandResponse::BulkString(v) if v == &Bytes::from("c")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_linsert() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("c")]);
        store.list_linsert(&key, true, &Bytes::from("c"), Bytes::from("b"));
        match store.list_lrange(&key, 0, -1) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(&items[1], CommandResponse::BulkString(v) if v == &Bytes::from("b")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_lrem() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("a"), Bytes::from("c"), Bytes::from("a")]);
        let r = store.list_lrem(&key, 2, &Bytes::from("a"));
        assert!(matches!(r, CommandResponse::Integer(2)));
        assert!(matches!(store.list_llen(&key), CommandResponse::Integer(3)));
    }

    #[test]
    fn test_ltrim() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c"), Bytes::from("d")]);
        store.list_ltrim(&key, 1, 2);
        assert!(matches!(store.list_llen(&key), CommandResponse::Integer(2)));
    }

    #[test]
    fn test_auto_delete_empty_list() {
        let mut store = ShardStore::new();
        let key = Bytes::from("list");
        store.list_rpush(&key, vec![Bytes::from("a")]);
        store.list_lpop(&key, 1);
        assert!(!store.exists(&key));
    }
}
