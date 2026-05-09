# SEC-012 — `CLUSTER MEET` lets any authenticated client direct the server to connect to arbitrary host:port (SSRF)

**Severity:** Medium

## Location

- `crates/solikv-server/src/resp_server.rs:392-411` (`CLUSTER MEET` subcommand handler)
- `crates/solikv-cluster/src/cluster.rs:90-99` (`ClusterManager::meet`)
- `crates/solikv-cluster/src/gossip.rs:300-302` (`GossipState::handle_meet`)

## Issue

When cluster mode is enabled, any authenticated RESP client can send

```
CLUSTER MEET 169.254.169.254 80
CLUSTER MEET internal-redis.prod.svc.cluster.local 6379
CLUSTER MEET 127.0.0.1 22
```

and the server will register that endpoint as a cluster peer, which (once the
gossip ping/pong loop kicks in) causes it to open outbound TCP connections to
the supplied address. Concretely this gives the attacker:

- **SSRF probing** of the server's internal network — useful against IMDS
  endpoints (`169.254.169.254`), private services, Kubernetes service IPs,
  cluster-internal DNS.
- **Topology poisoning** — the new peer can reply with arbitrary `UPDATE`
  messages and reshape slot ownership (see SEC-009 for the integrity gap).
- **Connection amplification** to internal services (a side-channel for port
  scanning the server's network).

Today the only restrictions are:

- The IP arrives via `arg_str` and is not validated (`resp_server.rs:401`).
- `port` parses to `u16`, defaulting to 7000 on failure.

There is no allow-list, no DNS-rebinding protection, no refusal of loopback
or link-local targets.

## Fix

1. Validate the `ip` argument:
   - Parse as `IpAddr` (reject hostnames, or only allow them when an explicit
     opt-in flag is set).
   - Reject loopback (unless we ourselves bind loopback), link-local
     (`169.254.0.0/16`), multicast, broadcast, unspecified (`0.0.0.0`).
   - Reject the metadata-service ranges by default (`169.254.169.254`,
     `fd00:ec2::254`).
2. Optionally introduce an operator-provided allow-list:
   `--cluster-meet-allow <CIDR>[,<CIDR>...]`.
3. When cluster auth is enabled (per SEC-009), require the new peer to
   complete the gossip-secret handshake before any outbound traffic to it is
   considered "real" cluster traffic.
4. Audit the engine path and disallow `CLUSTER MEET` for non-admin clients
   once a future ACL system exists (out of scope here, file follow-up).

## Verification

- Unit: `CLUSTER MEET 169.254.169.254 80` returns
  `ERR cluster meet target rejected (link-local)`.
- Unit: `CLUSTER MEET 10.0.0.5 6379` succeeds (RFC1918 is allowed by default).
