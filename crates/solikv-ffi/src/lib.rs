mod hashmap;

use hashmap::HashMap;
use std::sync::{Mutex, OnceLock};

static KV_STORE: OnceLock<Mutex<HashMap>> = OnceLock::new();

fn get_store() -> &'static Mutex<HashMap> {
    KV_STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

const MAX_KEY_LEN: usize = 4096;
const MAX_VALUE_LEN: usize = 65536;

fn validate_ptr<T>(ptr: *const T, len: usize) -> Option<()> {
    if ptr.is_null() {
        return None;
    }
    if len > MAX_KEY_LEN || len > MAX_VALUE_LEN {
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
    if validate_ptr(key, key_len).is_none() || validate_ptr(value, value_len).is_none() {
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
    if validate_ptr(key, key_len).is_none() || value_out.is_null() {
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
    if validate_ptr(key, key_len).is_none() {
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
    if validate_ptr(key, key_len).is_none() {
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
