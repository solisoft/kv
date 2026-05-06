# SEC-010: solikv-ffi uses `static mut` global and unchecked `from_raw_parts` on caller pointers

- **Severity:** Medium
- **Status:** Todo
- **Location:** `crates/solikv-ffi/src/lib.rs:7-21`, `lib.rs:29-86`

**Issue:** The FFI layer is unsound on two fronts:

1. **Mutable shared global without synchronization.** `static mut KV_STORE: Option<HashMap>` is accessed from `get_store()` via raw `unsafe` blocks, and every `solikv_*` extern function calls it. Any two threads in the host process calling `solikv_set` / `solikv_get` simultaneously race the same `HashMap` — undefined behaviour (data race) in Rust's model, with realistic outcomes being torn writes to `keys`/`values` arrays, missed entries, and process crashes. Rust 2024 will hard-error on `static mut`; even today this is UB.
2. **Unvalidated raw pointers.** `solikv_set`, `solikv_get`, `solikv_del`, `solikv_exists` all do `core::slice::from_raw_parts(ptr, len)` on pointers supplied by the C caller without any null/alignment check. A null pointer with `len > 0` is immediate UB; a non-null but wild pointer reads past mapped memory and segfaults the host process. `solikv_get`'s `core::slice::from_raw_parts_mut(value_out, copy_len)` has the same shape on the output buffer.

The crate is also `#![no_std]` with a `#[panic_handler]` that infinite-loops, so any panic in the dependency tree (none today, but easy to introduce) will hang the host thread instead of returning an error code.

**Fix:** Replace `static mut` with `OnceLock<Mutex<HashMap>>` (or per-thread state with explicit sync). At each FFI entry point, return `-1` early if any `*const u8` or `*mut u8` is null, and clamp `len` to a configured maximum before the `from_raw_parts`. Document the threading contract in the C header. Reconsider the `no_std` + custom panic handler choice — most consumers will tolerate `std`.
