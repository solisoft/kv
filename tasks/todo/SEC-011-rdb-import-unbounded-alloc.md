# SEC-011 — Redis RDB importer allocates `Vec<u8>` of attacker-controlled length, allowing OOM via crafted dump file

**Severity:** Medium

## Location

- `crates/solikv-persist/src/redis_rdb/parser.rs:152-184` (`read_string`)
- `crates/solikv-persist/src/redis_rdb/parser.rs:187-218` (`read_string_raw`)
- `crates/solikv-persist/src/redis_rdb/parser.rs:108-115` (64-bit length encoding read directly into `usize`)

## Issue

The RDB parser reads length-prefix fields and immediately allocates:

```rust
let mut buf = vec![0u8; len];               // line 155, 178, 190, 213
self.read_exact(&mut buf)?;
```

`len` for the 64-bit case (lines 110-115) is `u64::from_be_bytes(...) as usize`,
i.e. the raw value from the file with no upper bound and a silently-truncating
cast on 32-bit targets. A malicious `dump.rdb` with `len = 0x7FFF_FFFF_FFFF_FFFF`
triggers an immediate `vec![0u8; 9_223_372_036_854_775_807]`, which either
panics on allocation failure or pages the box into the abyss.

Trust model: the import is enabled via the operator-supplied
`--import-redis-rdb <PATH>` flag (`main.rs:308`), so the attacker must convince
the operator to import a hostile file. That is exactly the migration scenario
the README advertises ("Generate a dump from your Redis instance and load it at
startup") — operators *do* import files received from third parties.

The same pattern shows up in `read_string`'s LZF branch (`compressed_len` on
line 178) and again in `skip` (line 252) where `n` is a parameter to the
function but originates from file lengths.

## Fix

1. Define a sane maximum-string length constant (suggested 1 GiB, well beyond
   real Redis values) and reject larger values with an `InvalidData` error
   *before* allocating.
2. Same for `compressed_len` and `uncompressed_len` (LZF path).
3. Use `Vec::try_reserve_exact` followed by manual `set_len` after `read_exact`,
   so allocation failure returns `Err` instead of aborting.
4. For the 64-bit length encoding, reject values that exceed the cap *or* that
   don't fit in `usize` on the current target.

## Verification

- New test: feed a synthesized RDB header with an oversized `0b10_000001`
  length and assert the importer returns `Err`, not panic.
- Existing import tests still pass.
