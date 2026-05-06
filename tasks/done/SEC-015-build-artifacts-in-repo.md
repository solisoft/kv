# SEC-015: Build artifacts committed at repo root despite .gitignore

- **Severity:** Low
- **Status:** Todo
- **Location:** repository root, `.gitignore`

**Issue:** `git status` at audit time shows several rustc build artifacts sitting next to `Cargo.toml`:

```
?? lib.rmeta
?? solikv_ffi-3c0bbb04f5b260d7.solikv_ffi.70ba0336ca67ac8a-cgu.0.rcgu.o
?? solikv_ffi-3c0bbb04f5b260d7.solikv_ffi.70ba0336ca67ac8a-cgu.1.rcgu.o
?? solikv_ffi-3c0bbb04f5b260d7.solikv_ffi.70ba0336ca67ac8a-cgu.2.rcgu.o
```

`.gitignore` only excludes `target`, `data`, `tasks` — these stray artifacts sit *outside* `target/` (likely from a misconfigured `cargo build --out-dir .` or an earlier no_std experiment). They are not currently committed but `git add -A` from any contributor would push them. `.rmeta` and `.rcgu.o` files leak crate-internal metadata, debug symbols, source paths, and (on debuggable builds) inlined source snippets — useful to an attacker triaging a deployed binary.

**Fix:** Delete the stray files. Extend `.gitignore` to cover `*.rmeta`, `*.rcgu.o`, `*.o`, `*.rlib`, `*.dylib`, `*.so`, `*.dll`, and `*.pdb` at every depth. Consider adding a CI check that fails when these patterns appear outside `target/`.
