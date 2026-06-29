const CAPACITY: usize = 256;

pub struct HashMap {
    keys: [[u8; 64]; CAPACITY],
    values: [[u8; 128]; CAPACITY],
    key_lens: [usize; CAPACITY],
    value_lens: [usize; CAPACITY],
    used: [bool; CAPACITY],
    size: usize,
}

impl HashMap {
    pub fn new() -> Self {
        HashMap {
            keys: [[0; 64]; CAPACITY],
            values: [[0; 128]; CAPACITY],
            key_lens: [0; CAPACITY],
            value_lens: [0; CAPACITY],
            used: [false; CAPACITY],
            size: 0,
        }
    }

    fn hash(key: &[u8]) -> usize {
        let mut h: usize = 0;
        for &b in key {
            h = h.wrapping_mul(31).wrapping_add(b as usize);
        }
        h % CAPACITY
    }

    fn slot_for(&self, key: &[u8]) -> Option<usize> {
        let start = Self::hash(key);
        let mut idx = start;
        loop {
            if !self.used[idx] {
                return None;
            }
            if self.key_lens[idx] == key.len() && self.keys[idx][..key.len()] == *key {
                return Some(idx);
            }
            idx = (idx + 1) % CAPACITY;
            if idx == start {
                return None;
            }
        }
    }

    fn empty_slot(&self) -> Option<usize> {
        (0..CAPACITY).find(|&i| !self.used[i])
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        if let Some(idx) = self.slot_for(key) {
            self.value_lens[idx] = value.len().min(127);
            for (j, &b) in value.iter().enumerate().take(127) {
                self.values[idx][j] = b;
            }
            return;
        }

        if let Some(idx) = self.empty_slot() {
            self.key_lens[idx] = key.len().min(63);
            for (j, &b) in key.iter().enumerate().take(63) {
                self.keys[idx][j] = b;
            }
            self.value_lens[idx] = value.len().min(127);
            for (j, &b) in value.iter().enumerate().take(127) {
                self.values[idx][j] = b;
            }
            self.used[idx] = true;
            self.size += 1;
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(idx) = self.slot_for(key) {
            return Some(&self.values[idx][..self.value_lens[idx]]);
        }
        None
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        if let Some(idx) = self.slot_for(key) {
            self.used[idx] = false;
            self.key_lens[idx] = 0;
            self.value_lens[idx] = 0;
            self.size -= 1;
            return true;
        }
        false
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.slot_for(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn clear(&mut self) {
        for i in 0..CAPACITY {
            self.used[i] = false;
            self.key_lens[i] = 0;
            self.value_lens[i] = 0;
        }
        self.size = 0;
    }
}
