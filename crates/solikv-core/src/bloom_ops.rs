use bytes::Bytes;

use crate::store::ShardStore;
use crate::types::*;

/// Default capacity for auto-created bloom filters.
const DEFAULT_CAPACITY: u64 = 100;
/// Default error rate for auto-created bloom filters.
const DEFAULT_ERROR_RATE: f64 = 0.01;

impl ShardStore {
    fn get_or_create_bloom(&mut self, key: &Bytes) -> Result<&mut BloomFilterValue, CommandResponse> {
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::BloomFilter(_)) {
                return Err(CommandResponse::wrong_type());
            }
        }

        if self.get(key).is_none() {
            self.set(
                key.clone(),
                RedisValue::BloomFilter(BloomFilterValue::with_capacity(DEFAULT_CAPACITY, DEFAULT_ERROR_RATE)),
                None,
            );
        }

        match self.get_mut(key).unwrap() {
            KeyEntry { value: RedisValue::BloomFilter(ref mut bf), .. } => Ok(bf),
            _ => unreachable!(),
        }
    }

    fn get_bloom(&mut self, key: &Bytes) -> Result<Option<&BloomFilterValue>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::BloomFilter(bf) => Ok(Some(bf)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    /// BF.RESERVE key error_rate capacity
    pub fn bf_reserve(&mut self, key: &Bytes, error_rate: f64, capacity: u64) -> CommandResponse {
        if self.get(key).is_some() {
            return CommandResponse::error("ERR item exists");
        }
        let bf = BloomFilterValue::with_capacity(capacity, error_rate);
        self.set(key.clone(), RedisValue::BloomFilter(bf), None);
        CommandResponse::ok()
    }

    /// BF.ADD key item
    pub fn bf_add(&mut self, key: &Bytes, item: &Bytes) -> CommandResponse {
        let bf = match self.get_or_create_bloom(key) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let is_new = bf.add(item);
        CommandResponse::integer(if is_new { 1 } else { 0 })
    }

    /// BF.MADD key item [item ...]
    pub fn bf_madd(&mut self, key: &Bytes, items: &[Bytes]) -> CommandResponse {
        let bf = match self.get_or_create_bloom(key) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let results: Vec<CommandResponse> = items
            .iter()
            .map(|item| {
                let is_new = bf.add(item);
                CommandResponse::integer(if is_new { 1 } else { 0 })
            })
            .collect();
        CommandResponse::array(results)
    }

    /// BF.EXISTS key item
    pub fn bf_exists(&mut self, key: &Bytes, item: &Bytes) -> CommandResponse {
        match self.get_bloom(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::integer(0),
            Ok(Some(bf)) => CommandResponse::integer(if bf.exists(item) { 1 } else { 0 }),
        }
    }

    /// BF.MEXISTS key item [item ...]
    pub fn bf_mexists(&mut self, key: &Bytes, items: &[Bytes]) -> CommandResponse {
        match self.get_bloom(key) {
            Err(e) => e,
            Ok(None) => {
                let results = items.iter().map(|_| CommandResponse::integer(0)).collect();
                CommandResponse::array(results)
            }
            Ok(Some(bf)) => {
                let results = items
                    .iter()
                    .map(|item| CommandResponse::integer(if bf.exists(item) { 1 } else { 0 }))
                    .collect();
                CommandResponse::array(results)
            }
        }
    }

    /// BF.INFO key
    pub fn bf_info(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_bloom(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::error("ERR not found"),
            Ok(Some(bf)) => CommandResponse::array(vec![
                CommandResponse::bulk_string("Capacity"),
                CommandResponse::integer(bf.capacity as i64),
                CommandResponse::bulk_string("Size"),
                CommandResponse::integer(bf.bits.len() as i64),
                CommandResponse::bulk_string("Number of filters"),
                CommandResponse::integer(1),
                CommandResponse::bulk_string("Number of items inserted"),
                CommandResponse::integer(bf.num_items as i64),
                CommandResponse::bulk_string("Expansion rate"),
                CommandResponse::integer(0),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bf_add_exists() {
        let mut store = ShardStore::new();
        let key = Bytes::from("bf");

        let r = store.bf_add(&key, &Bytes::from("hello"));
        assert!(matches!(r, CommandResponse::Integer(1)));

        let r = store.bf_exists(&key, &Bytes::from("hello"));
        assert!(matches!(r, CommandResponse::Integer(1)));

        let r = store.bf_exists(&key, &Bytes::from("world"));
        assert!(matches!(r, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_bf_reserve() {
        let mut store = ShardStore::new();
        let key = Bytes::from("bf");

        let r = store.bf_reserve(&key, 0.001, 10000);
        assert!(matches!(r, CommandResponse::Ok));

        // Reserve on existing key should fail
        let r = store.bf_reserve(&key, 0.001, 10000);
        assert!(matches!(r, CommandResponse::Error(_)));
    }

    #[test]
    fn test_bf_add_duplicate() {
        let mut store = ShardStore::new();
        let key = Bytes::from("bf");
        store.bf_reserve(&key, 0.01, 1000);

        let r = store.bf_add(&key, &Bytes::from("item1"));
        assert!(matches!(r, CommandResponse::Integer(1)));

        // Adding same item again should return 0
        let r = store.bf_add(&key, &Bytes::from("item1"));
        assert!(matches!(r, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_bf_false_positive_rate() {
        let mut store = ShardStore::new();
        let key = Bytes::from("bf");
        let capacity = 10000u64;
        let error_rate = 0.01;
        store.bf_reserve(&key, error_rate, capacity);

        // Add capacity items
        for i in 0..capacity {
            store.bf_add(&key, &Bytes::from(format!("item:{}", i)));
        }

        // Check false positive rate on items NOT in the filter
        let test_count = 10000;
        let mut false_positives = 0;
        for i in 0..test_count {
            let item = Bytes::from(format!("notinfilter:{}", i));
            if let CommandResponse::Integer(1) = store.bf_exists(&key, &item) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;
        // Allow up to 3x the configured error rate for statistical variance
        assert!(
            fp_rate < error_rate * 3.0,
            "False positive rate too high: {} (expected < {})",
            fp_rate,
            error_rate * 3.0
        );
    }

    #[test]
    fn test_bf_madd_mexists() {
        let mut store = ShardStore::new();
        let key = Bytes::from("bf");
        let items = vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")];

        let r = store.bf_madd(&key, &items);
        match r {
            CommandResponse::Array(arr) => {
                assert_eq!(arr.len(), 3);
                for item in &arr {
                    assert!(matches!(item, CommandResponse::Integer(1)));
                }
            }
            _ => panic!("expected array"),
        }

        let check = vec![Bytes::from("a"), Bytes::from("d")];
        let r = store.bf_mexists(&key, &check);
        match r {
            CommandResponse::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(arr[0], CommandResponse::Integer(1)));
                assert!(matches!(arr[1], CommandResponse::Integer(0)));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_bf_info() {
        let mut store = ShardStore::new();
        let key = Bytes::from("bf");
        store.bf_reserve(&key, 0.01, 1000);
        store.bf_add(&key, &Bytes::from("a"));
        store.bf_add(&key, &Bytes::from("b"));

        let r = store.bf_info(&key);
        match r {
            CommandResponse::Array(arr) => {
                assert_eq!(arr.len(), 10);
                // Check "Number of items inserted" = 2
                assert!(matches!(&arr[7], CommandResponse::Integer(2)));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_bf_wrongtype() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mystr");
        store.set(key.clone(), RedisValue::String(Bytes::from("hello")), None);
        let r = store.bf_add(&key, &Bytes::from("a"));
        assert!(matches!(r, CommandResponse::Error(_)));
    }

    #[test]
    fn test_bf_exists_nonexistent_key() {
        let mut store = ShardStore::new();
        let key = Bytes::from("nokey");
        let r = store.bf_exists(&key, &Bytes::from("a"));
        assert!(matches!(r, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_bf_mexists_nonexistent_key() {
        let mut store = ShardStore::new();
        let key = Bytes::from("nokey");
        let r = store.bf_mexists(&key, &[Bytes::from("a"), Bytes::from("b")]);
        match r {
            CommandResponse::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert!(matches!(arr[0], CommandResponse::Integer(0)));
                assert!(matches!(arr[1], CommandResponse::Integer(0)));
            }
            _ => panic!("expected array"),
        }
    }
}
