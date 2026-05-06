# SEC-011: install.sh fetches and installs a release tarball with no checksum or signature verification

- **Severity:** Medium
- **Status:** Todo
- **Location:** `install.sh:52-86`

**Issue:** The installer is the documented "curl … | sh" path (line 3). It fetches the latest tag from `api.github.com/repos/solisoft/kv/releases/latest`, downloads `solikv-<os>-<arch>.tar.gz` from `github.com/solisoft/kv/releases/download/<tag>/`, untars into `mktemp -d`, and runs `install -m 755` on the resulting binary. There is:

- No SHA-256 (or any) checksum verification of the tarball.
- No GPG / minisign / cosign signature check.
- A silent fallback to `TAG="v0.1.0"` if the GitHub API call fails (line 61-63), so an attacker who can disrupt only the API call (squid blackhole, intermittent DNS) downgrades the install to the oldest published release.
- `tar xzf` without `--no-same-owner`, `--no-same-permissions`, or path-traversal guarding — a malicious tarball could write arbitrary files within `$TMP_DIR` (limited blast radius today, but reduces defense in depth).

If the GitHub release artifact is ever replaced (compromised maintainer account, account takeover, supply-chain incident) every fresh install gets RCE on first invocation.

**Fix:** Publish a `SHA256SUMS` (and ideally a `SHA256SUMS.asc`) alongside each release. Have the installer fetch both, verify the tarball checksum with `sha256sum -c` (or shasum), and refuse to install on mismatch. Drop the `v0.1.0` silent fallback — fail loudly when the latest tag cannot be fetched. Pass `--no-same-owner` to `tar`.
