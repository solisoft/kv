# SEC-008 — No TLS support; passwords and data travel in cleartext on both RESP and REST ports

**Severity:** Medium

## Location

- `crates/solikv-server/src/resp_server.rs:28` (`TcpListener::bind`)
- `crates/solikv-server/src/rest_server.rs:84-86` (`tokio::net::TcpListener` + `axum::serve`)
- `Cargo.toml` workspace deps — no `rustls`, `tokio-rustls`, `axum-server` w/ TLS.

## Issue

SoliKV speaks plaintext RESP on the wire and plaintext HTTP on the REST port.
There is no TLS support and no documented "front it with a proxy" guidance.

Concrete consequences:

- The RESP `AUTH` password is sent verbatim over the wire — passive sniffing on
  any network segment yields the credential.
- REST API uses HTTP Bearer tokens (`rest_server.rs:106-115`); same story.
- Every key and value transits in cleartext, including data placed via SET
  from clients in another datacenter or VPC peer.

For a Redis-compatible KV intended to be reachable across hosts (the README
documents Docker run patterns), no-TLS is a noticeable gap. Redis itself
supports TLS since 6.0.

## Fix

Add optional TLS termination for both servers using `tokio-rustls`:

1. New CLI flags: `--tls-cert <path>`, `--tls-key <path>`, optional
   `--tls-ca-file <path>` (for client cert verification), `--tls-port <port>`
   to bind a TLS-only listener (so plaintext can stay for local clients).
2. Mirror flags for REST: `--rest-tls-cert`, `--rest-tls-key`, etc., or share
   the same cert if both ports want TLS.
3. Use `axum-server` with rustls for the REST side; thread a
   `tokio_rustls::TlsAcceptor` into the RESP accept loop in `resp_server.rs`
   for the RESP side.
4. Document `tlsv1.2`+ minimum, support PEM cert reload via SIGHUP if cheap to
   wire up.

Add an integration test that performs a TLS handshake and a `PING`.

This is a non-trivial feature; consider scoping it as a separate PR after the
higher-severity tickets land.

## Verification

- New integration test using a self-signed cert.
- `cargo clippy --quiet -- -D warnings` clean with new feature.
- README and `www/app/views/docs/getting_started.html.slv` updated.
