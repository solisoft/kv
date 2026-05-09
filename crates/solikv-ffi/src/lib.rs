// FFI entry points dereference caller-supplied raw pointers after a null
// + length check. They could be marked `unsafe extern "C"` but that does
// nothing for C callers, so we silence clippy's structural lint instead.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::missing_safety_doc)]

mod hashmap;

use hashmap::HashMap;
use std::sync::{Mutex, OnceLock};

static KV_STORE: OnceLock<Mutex<HashMap>> = OnceLock::new();

fn get_store() -> &'static Mutex<HashMap> {
    KV_STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

const MAX_KEY_LEN: usize = 4096;
const MAX_VALUE_LEN: usize = 65536;

fn validate_key<T>(ptr: *const T, len: usize) -> Option<()> {
    if ptr.is_null() || len > MAX_KEY_LEN {
        return None;
    }
    Some(())
}

fn validate_value<T>(ptr: *const T, len: usize) -> Option<()> {
    if ptr.is_null() || len > MAX_VALUE_LEN {
        return None;
    }
    Some(())
}

#[no_mangle]
pub extern "C" fn solikv_init() -> i32 {
    get_store();
    0
}

#[no_mangle]
pub extern "C" fn solikv_set(
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if validate_key(key, key_len).is_none() || validate_value(value, value_len).is_none() {
        return -1;
    }

    let key_slice = unsafe { core::slice::from_raw_parts(key, key_len) };
    let value_slice = unsafe { core::slice::from_raw_parts(value, value_len) };

    if let Ok(mut store) = get_store().lock() {
        store.set(key_slice, value_slice);
        0
    } else {
        -1
    }
}

#[no_mangle]
pub extern "C" fn solikv_get(
    key: *const u8,
    key_len: usize,
    value_out: *mut u8,
    value_max: usize,
) -> i32 {
    if validate_key(key, key_len).is_none() || value_out.is_null() {
        return -1;
    }

    let store = match get_store().lock() {
        Ok(s) => s,
        Err(_) => return -1,
    };

    let key_slice = unsafe { core::slice::from_raw_parts(key, key_len) };

    if let Some(val) = store.get(key_slice) {
        let copy_len = val.len().min(value_max.min(MAX_VALUE_LEN));
        let val_slice = unsafe { core::slice::from_raw_parts_mut(value_out, copy_len) };
        val_slice.copy_from_slice(&val[..copy_len]);
        copy_len as i32
    } else {
        -1
    }
}

#[no_mangle]
pub extern "C" fn solikv_del(key: *const u8, key_len: usize) -> i32 {
    if validate_key(key, key_len).is_none() {
        return -1;
    }

    let key_slice = unsafe { core::slice::from_raw_parts(key, key_len) };

    if let Ok(mut store) = get_store().lock() {
        if store.remove(key_slice) {
            1
        } else {
            0
        }
    } else {
        -1
    }
}

#[no_mangle]
pub extern "C" fn solikv_exists(key: *const u8, key_len: usize) -> i32 {
    if validate_key(key, key_len).is_none() {
        return -1;
    }

    let key_slice = unsafe { core::slice::from_raw_parts(key, key_len) };

    if let Ok(store) = get_store().lock() {
        if store.contains_key(key_slice) {
            1
        } else {
            0
        }
    } else {
        -1
    }
}

#[no_mangle]
pub extern "C" fn solikv_count() -> i32 {
    if let Ok(store) = get_store().lock() {
        store.len() as i32
    } else {
        -1
    }
}

#[no_mangle]
pub extern "C" fn solikv_clear() -> i32 {
    if let Ok(mut store) = get_store().lock() {
        store.clear();
        0
    } else {
        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // SEC-010: a null key pointer must return -1, never UB.
    #[test]
    fn test_set_rejects_null_key() {
        let val = b"v";
        let rc = solikv_set(std::ptr::null(), 1, val.as_ptr(), val.len());
        assert_eq!(rc, -1);
    }

    // SEC-010: a null value pointer must return -1, never UB.
    #[test]
    fn test_set_rejects_null_value() {
        let key = b"k";
        let rc = solikv_set(key.as_ptr(), key.len(), std::ptr::null(), 1);
        assert_eq!(rc, -1);
    }

    // SEC-010: a key longer than MAX_KEY_LEN must return -1.
    #[test]
    fn test_set_rejects_oversized_key() {
        let big = vec![0u8; MAX_KEY_LEN + 1];
        let val = b"v";
        let rc = solikv_set(big.as_ptr(), big.len(), val.as_ptr(), val.len());
        assert_eq!(rc, -1);
    }

    // SEC-010: a value can be up to MAX_VALUE_LEN bytes — bigger than
    // MAX_KEY_LEN. Verify the validators don't conflate the two limits.
    #[test]
    fn test_set_accepts_value_above_key_limit() {
        let _ = solikv_init();
        let key = b"k1";
        let val = vec![0xABu8; MAX_KEY_LEN + 16]; // > MAX_KEY_LEN, ≤ MAX_VALUE_LEN
        let rc = solikv_set(key.as_ptr(), key.len(), val.as_ptr(), val.len());
        assert_eq!(rc, 0, "set must accept values above MAX_KEY_LEN");
    }
}
