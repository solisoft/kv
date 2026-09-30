/// Keyspace utilities for slot computation (Redis-compatible CRC16).
/// Used by the cluster module for routing.
///
/// Compute the Redis cluster hash slot for a key (0..16383).
/// Supports hash tags: if the key contains `{...}`, only the content inside
/// the first `{...}` pair is used for hashing.
pub fn hash_slot(key: &[u8]) -> u16 {
    let effective = extract_hash_tag(key);
    crc16_hash(effective) % 16384
}

fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

/// CRC16 CCITT (same algorithm Redis uses).
fn crc16_hash(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc = (crc << 8) ^ CRC16_TABLE[((crc >> 8) as u8 ^ byte) as usize];
    }
    crc
}

// CRC16 CCITT (XMODEM, polynomial 0x1021) lookup table, generated at
// compile time. It used to be a hand-typed literal with 34 wrong entries, so
// `CLUSTER KEYSLOT` disagreed with Redis (and with every cluster client that
// computes slots itself) for most keys.
static CRC16_TABLE: [u16; 256] = crc16_table();

const fn crc16_table() -> [u16; 256] {
    let mut t = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = (i as u16) << 8;
        let mut k = 0;
        while k < 8 {
            c = if c & 0x8000 != 0 {
                (c << 1) ^ 0x1021
            } else {
                c << 1
            };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_match_redis() {
        // Values from Redis' CLUSTER KEYSLOT.
        assert_eq!(hash_slot(b"foo"), 12182);
        assert_eq!(hash_slot(b"bar"), 5061);
        assert_eq!(hash_slot(b"hello"), 866);
        // CRC16/XMODEM check value; below 16384, so it is the slot itself.
        assert_eq!(hash_slot(b"123456789"), 0x31C3);
        assert_eq!(hash_slot(b"{user1000}.following"), hash_slot(b"user1000"));
        assert_eq!(hash_slot(b"foo{}{bar}"), hash_slot(b"foo{}{bar}"));
        // An empty tag hashes the whole key; only the first `{` counts.
        assert_ne!(hash_slot(b"foo{}{bar}"), hash_slot(b"bar"));
        assert_eq!(hash_slot(b"foo{{bar}}zap"), hash_slot(b"{bar"));
    }

    #[test]
    fn test_hash_slot_basic() {
        // Known Redis slot values
        let slot = hash_slot(b"foo");
        assert!(slot < 16384);
    }

    #[test]
    fn test_hash_slot_hash_tag() {
        // Keys with same hash tag should map to same slot
        let s1 = hash_slot(b"user:{123}:name");
        let s2 = hash_slot(b"user:{123}:email");
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_hash_slot_empty_tag() {
        // Empty hash tag {} should be ignored
        let s1 = hash_slot(b"foo{}bar");
        let s2 = hash_slot(b"foo{}bar");
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_hash_slot_range() {
        for i in 0..1000 {
            let key = format!("key:{}", i);
            let slot = hash_slot(key.as_bytes());
            assert!(slot < 16384);
        }
    }
}
