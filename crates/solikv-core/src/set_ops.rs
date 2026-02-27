use bytes::Bytes;
use std::collections::HashSet;

use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_set(&mut self, key: &Bytes) -> Result<&mut HashSet<Bytes>, CommandResponse> {
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::Set(_)) {
                return Err(CommandResponse::wrong_type());
            }
        }

        if self.get(key).is_none() {
            self.set(key.clone(), RedisValue::Set(HashSet::new()), None);
        }

        match self.get_mut(key).unwrap() {
            KeyEntry { value: RedisValue::Set(ref mut s), .. } => Ok(s),
            _ => unreachable!(),
        }
    }

    fn get_set(&mut self, key: &Bytes) -> Result<Option<&HashSet<Bytes>>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Set(s) => Ok(Some(s)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    fn get_set_cloned(&mut self, key: &Bytes) -> Result<HashSet<Bytes>, CommandResponse> {
        match self.get(key) {
            None => Ok(HashSet::new()),
            Some(entry) => match &entry.value {
                RedisValue::Set(s) => Ok(s.clone()),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    pub fn set_sadd(&mut self, key: &Bytes, members: Vec<Bytes>) -> CommandResponse {
        let set = match self.get_or_create_set(key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let mut added = 0i64;
        for member in members {
            if set.insert(member) {
                added += 1;
            }
        }
        CommandResponse::integer(added)
    }

    pub fn set_srem(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::Set(set) => {
                let mut removed = 0i64;
                for member in members {
                    if set.remove(member) {
                        removed += 1;
                    }
                }
                if set.is_empty() {
                    self.del(key);
                }
                CommandResponse::integer(removed)
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn set_smembers(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::array(vec![]),
            Ok(Some(set)) => {
                let items = set.iter().map(|m| CommandResponse::bulk(m.clone())).collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn set_sismember(&mut self, key: &Bytes, member: &Bytes) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(set)) => CommandResponse::integer(if set.contains(member) { 1 } else { 0 }),
        }
    }

    pub fn set_scard(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(set)) => CommandResponse::integer(set.len() as i64),
        }
    }

    pub fn set_sinter(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::error("ERR wrong number of arguments for 'sinter' command");
        }
        let first = match self.get_set_cloned(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };

        let mut result = first;
        for key in &keys[1..] {
            let other = match self.get_set_cloned(key) {
                Ok(s) => s,
                Err(e) => return e,
            };
            result = result.intersection(&other).cloned().collect();
        }

        let items = result.into_iter().map(|m| CommandResponse::bulk(m)).collect();
        CommandResponse::array(items)
    }

    pub fn set_sunion(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::error("ERR wrong number of arguments for 'sunion' command");
        }
        let mut result = HashSet::new();
        for key in keys {
            match self.get_set_cloned(key) {
                Ok(s) => result.extend(s),
                Err(e) => return e,
            }
        }
        let items = result.into_iter().map(|m| CommandResponse::bulk(m)).collect();
        CommandResponse::array(items)
    }

    pub fn set_sdiff(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.is_empty() {
            return CommandResponse::error("ERR wrong number of arguments for 'sdiff' command");
        }
        let mut result = match self.get_set_cloned(&keys[0]) {
            Ok(s) => s,
            Err(e) => return e,
        };
        for key in &keys[1..] {
            let other = match self.get_set_cloned(key) {
                Ok(s) => s,
                Err(e) => return e,
            };
            result = result.difference(&other).cloned().collect();
        }
        let items = result.into_iter().map(|m| CommandResponse::bulk(m)).collect();
        CommandResponse::array(items)
    }

    pub fn set_spop(&mut self, key: &Bytes, count: usize) -> CommandResponse {
        let entry = match self.get_mut(key) {
            None => return if count == 1 { CommandResponse::nil() } else { CommandResponse::array(vec![]) },
            Some(e) => e,
        };
        match &mut entry.value {
            RedisValue::Set(set) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    // Get first element (not truly random but deterministic for testing)
                    let member = set.iter().next().cloned();
                    match member {
                        Some(m) => {
                            set.remove(&m);
                            result.push(CommandResponse::bulk(m));
                        }
                        None => break,
                    }
                }
                if set.is_empty() {
                    self.del(key);
                }
                if count == 1 {
                    result.into_iter().next().unwrap_or(CommandResponse::nil())
                } else {
                    CommandResponse::array(result)
                }
            }
            _ => CommandResponse::wrong_type(),
        }
    }

    pub fn set_srandmember(&mut self, key: &Bytes, count: Option<i64>) -> CommandResponse {
        match self.get_set(key) {
            Err(e) => e,
            Ok(None) => match count {
                None => CommandResponse::nil(),
                Some(_) => CommandResponse::array(vec![]),
            },
            Ok(Some(set)) => {
                let count = count.unwrap_or(1);
                let abs_count = count.unsigned_abs() as usize;
                let members: Vec<Bytes> = set.iter().take(abs_count).cloned().collect();

                if count >= 0 && count <= 1 && members.len() == 1 {
                    if count == 0 {
                        return CommandResponse::array(vec![]);
                    }
                    // Single element case without explicit count
                    return CommandResponse::bulk(members[0].clone());
                }
                let items = members.into_iter().map(|m| CommandResponse::bulk(m)).collect();
                CommandResponse::array(items)
            }
        }
    }

    pub fn set_smove(&mut self, src: &Bytes, dst: Bytes, member: Bytes) -> CommandResponse {
        // Remove from source
        let entry = match self.get_mut(src) {
            None => return CommandResponse::integer(0),
            Some(e) => e,
        };
        let removed = match &mut entry.value {
            RedisValue::Set(set) => set.remove(&member),
            _ => return CommandResponse::wrong_type(),
        };
        if !removed {
            return CommandResponse::integer(0);
        }
        // Clean up empty source
        if let Some(entry) = self.get(src) {
            if matches!(&entry.value, RedisValue::Set(s) if s.is_empty()) {
                self.del(src);
            }
        }
        // Add to destination
        let dest_set = match self.get_or_create_set(&dst) {
            Ok(s) => s,
            Err(e) => return e,
        };
        dest_set.insert(member);
        CommandResponse::integer(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sadd_scard() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        let r = store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("a")]);
        assert!(matches!(r, CommandResponse::Integer(2)));
        assert!(matches!(store.set_scard(&key), CommandResponse::Integer(2)));
    }

    #[test]
    fn test_srem() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        let r = store.set_srem(&key, &[Bytes::from("a")]);
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(store.set_scard(&key), CommandResponse::Integer(1)));
    }

    #[test]
    fn test_sismember() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a")]);
        assert!(matches!(store.set_sismember(&key, &Bytes::from("a")), CommandResponse::Integer(1)));
        assert!(matches!(store.set_sismember(&key, &Bytes::from("b")), CommandResponse::Integer(0)));
    }

    #[test]
    fn test_smembers() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        match store.set_smembers(&key) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sinter() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("s1"), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")]);
        match store.set_sinter(&[Bytes::from("s1"), Bytes::from("s2")]) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sunion() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("s1"), vec![Bytes::from("a"), Bytes::from("b")]);
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("b"), Bytes::from("c")]);
        match store.set_sunion(&[Bytes::from("s1"), Bytes::from("s2")]) {
            CommandResponse::Array(items) => assert_eq!(items.len(), 3),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sdiff() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("s1"), vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("b"), Bytes::from("c")]);
        match store.set_sdiff(&[Bytes::from("s1"), Bytes::from("s2")]) {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 1);
                assert!(matches!(&items[0], CommandResponse::BulkString(v) if v == &Bytes::from("a")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_spop() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a"), Bytes::from("b")]);
        let r = store.set_spop(&key, 1);
        assert!(matches!(r, CommandResponse::BulkString(_)));
        assert!(matches!(store.set_scard(&key), CommandResponse::Integer(1)));
    }

    #[test]
    fn test_smove() {
        let mut store = ShardStore::new();
        store.set_sadd(&Bytes::from("s1"), vec![Bytes::from("a"), Bytes::from("b")]);
        store.set_sadd(&Bytes::from("s2"), vec![Bytes::from("c")]);
        let r = store.set_smove(&Bytes::from("s1"), Bytes::from("s2"), Bytes::from("a"));
        assert!(matches!(r, CommandResponse::Integer(1)));
        assert!(matches!(store.set_scard(&Bytes::from("s1")), CommandResponse::Integer(1)));
        assert!(matches!(store.set_scard(&Bytes::from("s2")), CommandResponse::Integer(2)));
    }

    #[test]
    fn test_auto_delete_empty_set() {
        let mut store = ShardStore::new();
        let key = Bytes::from("set");
        store.set_sadd(&key, vec![Bytes::from("a")]);
        store.set_srem(&key, &[Bytes::from("a")]);
        assert!(!store.exists(&key));
    }
}
