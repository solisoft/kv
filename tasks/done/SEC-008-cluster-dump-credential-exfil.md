# SEC-008: cluster_dump re-uses `--cluster-password` against any host returned by an untrusted CLUSTER NODES response

- **Severity:** High
- **Status:** Todo
- **Location:** `crates/solikv-server/src/cluster_dump.rs:430-486`, `cluster_dump.rs:586-756`, `cluster_dump.rs:157-177`

**Issue:** `dump_cluster` and `restore_cluster` connect to the operator-supplied `--cluster-connect` address, send `CLUSTER NODES`, parse the textual response into `(ip, port)` pairs, then loop over each pair and call `connect_node(&node.ip, node.port, password)` — which sends `AUTH <password>` over plain TCP. Two problems:

1. **Trust transfer:** the seed node's `CLUSTER NODES` output entirely controls which IPs receive the cluster password next. If the seed is compromised (or the operator fat-fingers the seed address to a hostile IP), the password is exfiltrated to attacker-chosen endpoints — with no warning, since `connect_node` returns success on `+OK`.
2. **Plaintext AUTH:** the connection is `TcpStream::connect_timeout` with no TLS option, so the password also leaks to anything between the dumper and each "node".

The same loop exists in `restore_cluster` (which additionally writes data to whatever endpoints the dump file's header lists).

**Fix:** Pin the dumper to the seed address: only AUTH against hosts whose `(ip, port)` exactly matches the seed, or require an explicit `--allow-host <ip[:port]>` flag listing every endpoint allowed to receive the password. Add a TLS option for the dumper's RESP connections. Show a clear preview of the node list and require interactive confirmation before connecting to additional IPs.
