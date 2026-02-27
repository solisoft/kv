use bytes::Bytes;

use crate::store::ShardStore;
use crate::types::*;

impl ShardStore {
    fn get_or_create_hll(&mut self, key: &Bytes) -> Result<&mut HyperLogLogValue, CommandResponse> {
        if let Some(entry) = self.get(key) {
            if !matches!(entry.value, RedisValue::HyperLogLog(_)) {
                return Err(CommandResponse::wrong_type());
            }
        }

        if self.get(key).is_none() {
            self.set(key.clone(), RedisValue::HyperLogLog(HyperLogLogValue::new()), None);
        }

        match self.get_mut(key).unwrap() {
            KeyEntry { value: RedisValue::HyperLogLog(ref mut hll), .. } => Ok(hll),
            _ => unreachable!(),
        }
    }

    fn get_hll(&mut self, key: &Bytes) -> Result<Option<&HyperLogLogValue>, CommandResponse> {
        match self.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::HyperLogLog(hll) => Ok(Some(hll)),
                _ => Err(CommandResponse::wrong_type()),
            },
        }
    }

    /// PFADD key element [element ...]
    pub fn pfadd(&mut self, key: &Bytes, elements: &[Bytes]) -> CommandResponse {
        let hll = match self.get_or_create_hll(key) {
            Ok(h) => h,
            Err(e) => return e,
        };
        let mut changed = false;
        for elem in elements {
            if hll.add(elem) {
                changed = true;
            }
        }
        CommandResponse::integer(if changed { 1 } else { 0 })
    }

    /// PFCOUNT key [key ...]
    /// Single key: return count from that HLL.
    /// Multiple keys: merge into temp HLL, return count.
    pub fn pfcount(&mut self, keys: &[Bytes]) -> CommandResponse {
        if keys.len() == 1 {
            match self.get_hll(&keys[0]) {
                Err(e) => return e,
                Ok(None) => return CommandResponse::integer(0),
                Ok(Some(hll)) => return CommandResponse::integer(hll.count() as i64),
            }
        }

        // Multiple keys: merge into temporary HLL
        let mut merged = HyperLogLogValue::new();
        for key in keys {
            match self.get_hll(key) {
                Err(e) => return e,
                Ok(None) => {} // treat missing as empty
                Ok(Some(hll)) => merged.merge(hll),
            }
        }
        CommandResponse::integer(merged.count() as i64)
    }

    /// Get raw HLL registers for cross-shard operations.
    pub fn pfget_registers(&mut self, key: &Bytes) -> CommandResponse {
        match self.get_hll(key) {
            Err(e) => e,
            Ok(None) => CommandResponse::Nil,
            Ok(Some(hll)) => CommandResponse::BulkString(Bytes::from(hll.registers.clone())),
        }
    }

    /// Set HLL registers directly (for cross-shard merge).
    pub fn pfset_registers(&mut self, key: &Bytes, registers: Vec<u8>) -> CommandResponse {
        let hll = HyperLogLogValue { registers };
        self.set(key.clone(), RedisValue::HyperLogLog(hll), None);
        CommandResponse::ok()
    }

    /// PFMERGE destkey sourcekey [sourcekey ...]
    pub fn pfmerge(&mut self, dest: &Bytes, sources: &[Bytes]) -> CommandResponse {
        // Collect source registers
        let mut merged = HyperLogLogValue::new();

        // First merge existing dest if it exists
        match self.get_hll(dest) {
            Err(e) => return e,
            Ok(Some(hll)) => merged.merge(hll),
            Ok(None) => {}
        }

        // Merge all sources
        for src in sources {
            match self.get_hll(src) {
                Err(e) => return e,
                Ok(Some(hll)) => merged.merge(hll),
                Ok(None) => {}
            }
        }

        // Store merged result into dest
        self.set(dest.clone(), RedisValue::HyperLogLog(merged), None);
        CommandResponse::ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pfadd_new_key() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hll");
        let r = store.pfadd(&key, &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        assert!(matches!(r, CommandResponse::Integer(1)));
    }

    #[test]
    fn test_pfadd_returns_change() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hll");
        store.pfadd(&key, &[Bytes::from("a")]);

        // Adding same element should return 0 (no change)
        let r = store.pfadd(&key, &[Bytes::from("a")]);
        assert!(matches!(r, CommandResponse::Integer(0)));

        // Adding new element should return 1
        let r = store.pfadd(&key, &[Bytes::from("b")]);
        assert!(matches!(r, CommandResponse::Integer(1)));
    }

    #[test]
    fn test_pfcount_empty() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hll");
        let r = store.pfcount(&[key]);
        assert!(matches!(r, CommandResponse::Integer(0)));
    }

    #[test]
    fn test_pfcount_accuracy() {
        let mut store = ShardStore::new();
        let key = Bytes::from("hll");
        let n = 10000;
        for i in 0..n {
            store.pfadd(&key, &[Bytes::from(format!("element:{}", i))]);
        }
        let count = match store.pfcount(&[key]) {
            CommandResponse::Integer(c) => c,
            _ => panic!("expected integer"),
        };
        // HLL with p=14 should be within ~2% of actual
        let error = ((count as f64 - n as f64) / n as f64).abs();
        assert!(error < 0.03, "HLL error too high: {} (count={}, expected={})", error, count, n);
    }

    #[test]
    fn test_pfmerge() {
        let mut store = ShardStore::new();
        let k1 = Bytes::from("hll1");
        let k2 = Bytes::from("hll2");
        store.pfadd(&k1, &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        store.pfadd(&k2, &[Bytes::from("c"), Bytes::from("d")]);

        let dest = Bytes::from("merged");
        let r = store.pfmerge(&dest, &[k1, k2]);
        assert!(matches!(r, CommandResponse::Ok));

        let count = match store.pfcount(&[dest]) {
            CommandResponse::Integer(c) => c,
            _ => panic!("expected integer"),
        };
        assert_eq!(count, 4);
    }

    #[test]
    fn test_pfcount_multi_key() {
        let mut store = ShardStore::new();
        let k1 = Bytes::from("hll1");
        let k2 = Bytes::from("hll2");
        store.pfadd(&k1, &[Bytes::from("a"), Bytes::from("b")]);
        store.pfadd(&k2, &[Bytes::from("b"), Bytes::from("c")]);

        let count = match store.pfcount(&[k1, k2]) {
            CommandResponse::Integer(c) => c,
            _ => panic!("expected integer"),
        };
        assert_eq!(count, 3);
    }

    #[test]
    fn test_pfadd_wrongtype() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mystr");
        store.set(key.clone(), RedisValue::String(Bytes::from("hello")), None);
        let r = store.pfadd(&key, &[Bytes::from("a")]);
        assert!(matches!(r, CommandResponse::Error(_)));
    }
}
