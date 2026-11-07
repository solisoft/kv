use axum::serve::ListenerExt;
use axum::{
    extract::{ConnectInfo, Path, Query, Request, State},
    http::StatusCode,
    middleware::Next,
    response::Json,
    routing::{delete, get, post, put},
    Router,
};
use bytes::Bytes;
use serde::Deserialize;
#[allow(unused_imports)]
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use solikv_core::CommandResponse;
use solikv_engine::response::response_to_json;
use solikv_engine::CommandEngine;

const MAX_AUTH_FAILURES_REST: u32 = 10;
const AUTH_COOLDOWN_SECS_REST: u64 = 30;

#[derive(Default)]
struct AuthFailureTracker {
    failures: RwLock<std::collections::HashMap<String, (u32, std::time::Instant)>>,
    /// `failures.len()`, readable without the lock: when nobody has failed
    /// (the normal case) an authenticated request touches no shared lock.
    len: std::sync::atomic::AtomicUsize,
}

impl AuthFailureTracker {
    #[inline]
    fn any(&self) -> bool {
        self.len.load(std::sync::atomic::Ordering::Relaxed) != 0
    }

    fn is_blocked(&self, ip: &str) -> bool {
        if !self.any() {
            return false;
        }
        let failures = self.failures.read().unwrap();
        if let Some((count, first_failure)) = failures.get(ip) {
            if *count >= MAX_AUTH_FAILURES_REST {
                let elapsed = first_failure.elapsed().as_secs();
                return elapsed < AUTH_COOLDOWN_SECS_REST;
            }
        }
        false
    }

    fn record_failure(&self, ip: &str) {
        let mut failures = self.failures.write().unwrap();
        let now = std::time::Instant::now();
        // Keyed by peer IP, so it grows with the number of addresses that
        // ever failed. Forget the ones whose cooldown is long over.
        if failures.len() >= AUTH_TRACKER_PRUNE_AT {
            failures.retain(|_, (_, last)| last.elapsed().as_secs() < AUTH_COOLDOWN_SECS_REST);
        }
        let entry = failures.entry(ip.to_string()).or_insert((0, now));
        entry.0 += 1;
        entry.1 = now;
        self.len
            .store(failures.len(), std::sync::atomic::Ordering::Relaxed);
    }

    fn record_success(&self, ip: &str) {
        // Every authenticated request lands here; only take a lock when
        // there is something to forget.
        if !self.any() || !self.failures.read().unwrap().contains_key(ip) {
            return;
        }
        let mut failures = self.failures.write().unwrap();
        failures.remove(ip);
        self.len
            .store(failures.len(), std::sync::atomic::Ordering::Relaxed);
    }
}

/// Prune the failure map once it holds this many addresses.
const AUTH_TRACKER_PRUNE_AT: usize = 4096;

#[derive(Clone)]
struct AppState {
    engine: Arc<CommandEngine>,
    password: Option<Arc<String>>,
    /// The ACL user a valid Bearer token acts as.
    token_user: Arc<String>,
    auth_tracker: Arc<AuthFailureTracker>,
}

tokio::task_local! {
    /// The ACL user of the request being served, set by `auth_middleware`
    /// around the handler (which runs in the same task).
    static REST_USER: Arc<solikv_engine::acl::AclUser>;
}

/// Run a command for the current request, under its user's ACL: a REST
/// route is checked exactly as the command it maps to would be over RESP.
fn exec(state: &AppState, cmd: &str, args: &[Bytes]) -> CommandResponse {
    let user = REST_USER.try_with(|u| u.clone()).ok();
    if let Some(user) = user.as_ref().filter(|u| !u.unrestricted()) {
        let acl = state.engine.acl();
        match solikv_engine::commands::lookup_ci(cmd.as_bytes()) {
            Some(spec) => {
                if let Err(denied) = acl.check(user, spec, args) {
                    acl.log_denial(
                        &user.name,
                        &denied,
                        solikv_engine::acl::Context::TopLevel,
                        format!("rest user={}", user.name),
                    );
                    return CommandResponse::Error(denied.message(&user.name));
                }
            }
            None => {
                return CommandResponse::Error(format!(
                    "ERR unknown command '{}'",
                    cmd.to_lowercase()
                ))
            }
        }
    }
    // Under the request's user, so what the command runs inside it — a
    // script's `redis.call`s, `SORT`'s patterns — is checked as over RESP.
    match &user {
        Some(user) => state.engine.execute_as(user, cmd, args),
        None => state.engine.execute(cmd, args),
    }
}

/// How the REST listener is exposed.
#[derive(Clone, Default)]
pub struct RestConfig {
    /// Bearer token. `None`: no token; requests then need `Basic`
    /// credentials of an ACL user, unless the `default` user needs no
    /// password (an open server).
    pub token: Option<Arc<String>>,
    /// The ACL user a valid Bearer token acts as (`None`: `default`).
    pub token_user: Option<String>,
    /// Most concurrent connections (0 = no limit).
    pub max_connections: usize,
    /// Terminate TLS with this configuration.
    pub tls: Option<rustls::ServerConfig>,
    /// Accepted `Host` header names (without port). `None` = no check.
    ///
    /// A browser can be made to send requests to a loopback port from any
    /// web page by DNS rebinding: the attacker's name resolves to 127.0.0.1,
    /// and the page then reads and writes every key. The `Host` header still
    /// carries the attacker's name, which is what this refuses.
    pub allowed_hosts: Option<Vec<String>>,
    /// Longest a TLS handshake may take.
    pub handshake_timeout: std::time::Duration,
    /// `/metrics` requires the token (when one is set).
    pub metrics_auth: bool,
}

/// Host names that always reach a loopback listener legitimately.
pub const LOOPBACK_HOSTS: &[&str] = &["localhost", "127.0.0.1", "::1"];

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
    password: Option<Arc<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with(
        addr,
        engine,
        RestConfig {
            token: password,
            handshake_timeout: std::time::Duration::from_secs(10),
            ..Default::default()
        },
    )
    .await
}

pub async fn run_with(
    addr: &str,
    engine: Arc<CommandEngine>,
    config: RestConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState {
        engine,
        password: config.token.clone(),
        token_user: Arc::new(
            config
                .token_user
                .clone()
                .unwrap_or_else(|| "default".to_string()),
        ),
        auth_tracker: Arc::new(AuthFailureTracker::default()),
    };
    let allowed_hosts: Arc<Option<Vec<String>>> = Arc::new(
        config
            .allowed_hosts
            .map(|hosts| hosts.into_iter().map(|h| h.to_ascii_lowercase()).collect()),
    );

    let metrics_auth = config.metrics_auth;
    let app = Router::new();
    let app = if metrics_auth {
        app.route("/metrics", get(metrics))
    } else {
        app
    };
    let app = app
        // Key-value operations
        .route("/kv/{key}", get(get_key))
        .route("/kv/{key}", put(set_key))
        .route("/kv/{key}", delete(delete_key))
        // List operations
        .route("/list/{key}", get(list_range))
        .route("/list/{key}/lpush", post(list_lpush))
        .route("/list/{key}/rpush", post(list_rpush))
        .route("/list/{key}/lpop", post(list_lpop))
        .route("/list/{key}/rpop", post(list_rpop))
        // Hash operations
        .route("/hash/{key}", get(hash_getall))
        .route("/hash/{key}/{field}", get(hash_get_field))
        .route("/hash/{key}", put(hash_set))
        .route("/hash/{key}/{field}", delete(hash_del_field))
        // Set operations
        .route("/set/{key}", get(set_members))
        .route("/set/{key}", put(set_add))
        .route("/set/{key}/{member}", delete(set_rem_member))
        // Sorted set operations
        .route("/zset/{key}", get(zset_range))
        .route("/zset/{key}", put(zset_add))
        // HyperLogLog operations
        .route("/pfadd/{key}", post(pfadd))
        .route("/pfcount/{key}", get(pfcount))
        .route("/pfmerge/{dest}", post(pfmerge))
        // Bitmap operations
        .route("/bitmap/{key}/setbit", post(bitmap_setbit))
        .route("/bitmap/{key}/getbit/{offset}", get(bitmap_getbit))
        .route("/bitmap/{key}/bitcount", get(bitmap_bitcount))
        // Bloom filter operations
        .route("/bf/reserve/{key}", post(bf_reserve))
        .route("/bf/add/{key}", post(bf_add))
        .route("/bf/exists/{key}/{item}", get(bf_exists))
        .route("/bf/info/{key}", get(bf_info))
        // Geospatial operations
        .route("/geo/{key}/add", post(geo_add))
        .route("/geo/{key}/pos/{member}", get(geo_pos))
        .route("/geo/{key}/dist/{member1}/{member2}", get(geo_dist))
        .route("/geo/{key}/search", post(geo_search))
        // Generic command endpoint
        .route("/cmd", post(execute_command))
        // Server info
        .route("/info", get(server_info))
        .route("/dbsize", get(dbsize))
        // Inside auth: a refused request writes nothing to wait for.
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            durable_middleware,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        // Probes need no credentials (and say nothing about the data).
        .route("/health", get(health))
        .route("/ready", get(ready));
    let app = if metrics_auth {
        app
    } else {
        app.route("/metrics", get(metrics))
    };
    let app = app
        // Outermost: a request for a foreign Host is refused before anything
        // else, including the auth failure accounting.
        .layer(axum::middleware::from_fn_with_state(
            allowed_hosts,
            host_middleware,
        ))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let limit = Arc::new(tokio::sync::Semaphore::new(
        if config.max_connections == 0 {
            tokio::sync::Semaphore::MAX_PERMITS
        } else {
            config.max_connections
        },
    ));
    match config.tls {
        Some(tls) => {
            tracing::info!("REST API listening on {} (TLS)", addr);
            let listener = TlsListener::spawn(listener, tls, config.handshake_timeout, limit);
            axum::serve(
                listener.tap_io(|_| {}),
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await?;
        }
        None => {
            tracing::info!("REST API listening on {}", addr);
            axum::serve(
                // `tap_io`: axum provides `ConnectInfo<SocketAddr>` for a
                // custom listener through it.
                CappedListener {
                    inner: listener,
                    limit,
                }
                .tap_io(|_| {}),
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await?;
        }
    }
    Ok(())
}

/// A connection holding one of the listener's slots until it closes.
pub struct Counted<S> {
    inner: S,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl<S: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for Counted<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for Counted<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// What a connection over `--rest-max-connections` gets before it is closed.
const REST_BUSY: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\n\
content-length: 39\r\nconnection: close\r\n\r\n{\"error\":\"too many REST connections\"}";

/// Refuse a connection over the cap: a 503, then close.
fn refuse_busy(mut socket: tokio::net::TcpStream, peer: SocketAddr) {
    tracing::warn!("REST connection limit reached, refusing {}", peer);
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            socket.write_all(REST_BUSY),
        )
        .await;
    });
}

/// The plain listener, with the connection cap.
struct CappedListener {
    inner: tokio::net::TcpListener,
    limit: Arc<tokio::sync::Semaphore>,
}

impl axum::serve::Listener for CappedListener {
    type Io = Counted<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (socket, peer) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::debug!("REST accept error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }
            };
            match self.limit.clone().try_acquire_owned() {
                Ok(slot) => {
                    let _ = socket.set_nodelay(true);
                    return (
                        Counted {
                            inner: socket,
                            _slot: slot,
                        },
                        peer,
                    );
                }
                Err(_) => refuse_busy(socket, peer),
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// A listener that hands axum TLS streams.
///
/// Handshakes run in their own tasks, bounded by a timeout, and only finished
/// ones reach `accept`: done inline, one client that opens a socket and never
/// completes the handshake would stall every connection behind it.
struct TlsListener {
    rx: tokio::sync::mpsc::Receiver<(
        Counted<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>,
        SocketAddr,
    )>,
    local: SocketAddr,
}

impl TlsListener {
    fn spawn(
        listener: tokio::net::TcpListener,
        config: rustls::ServerConfig,
        handshake_timeout: std::time::Duration,
        limit: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let local = listener
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            loop {
                let (socket, peer) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        tracing::debug!("REST accept error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };
                // The slot is taken before the handshake: half-open TLS
                // connections count too.
                let slot = match limit.clone().try_acquire_owned() {
                    Ok(slot) => slot,
                    Err(_) => {
                        tracing::warn!("REST connection limit reached, refusing {}", peer);
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(handshake_timeout, acceptor.accept(socket)).await {
                        Ok(Ok(stream)) => {
                            let _ = tx
                                .send((
                                    Counted {
                                        inner: stream,
                                        _slot: slot,
                                    },
                                    peer,
                                ))
                                .await;
                        }
                        Ok(Err(e)) => tracing::debug!("REST TLS handshake from {}: {}", peer, e),
                        Err(_) => tracing::debug!("REST TLS handshake from {} timed out", peer),
                    }
                });
            }
        });
        Self { rx, local }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = Counted<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(accepted) => accepted,
            // The accept task only ends with the process.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}

/// The host part of a `Host` header: `name`, `name:port`, `[v6]` or `[v6]:port`.
fn host_name(header: &str) -> &str {
    let header = header.trim();
    if let Some(rest) = header.strip_prefix('[') {
        return rest.split(']').next().unwrap_or("");
    }
    match header.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => header,
    }
}

async fn host_middleware(
    State(allowed): State<Arc<Option<Vec<String>>>>,
    req: Request,
    next: Next,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    if let Some(allowed) = allowed.as_ref() {
        // No Host header at all is an HTTP/1.0 client, not a browser — and a
        // rebinding attack needs a browser, which always sends one.
        if let Some(host) = req.headers().get("host") {
            let name = host
                .to_str()
                .map(|h| host_name(h).to_ascii_lowercase())
                .unwrap_or_default();
            if !allowed.contains(&name) {
                return Err((
                    StatusCode::MISDIRECTED_REQUEST,
                    Json(serde_json::json!({"error": "Host not allowed"})),
                ));
            }
        }
    }
    Ok(next.run(req).await)
}

/// `appendfsync always`: a response leaves only once the log holds what it
/// may reflect, as RESP replies do (see `CommandEngine::replies_durable`).
async fn durable_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> axum::response::Response {
    let resp = next.run(req).await;
    state.engine.replies_durable().await;
    resp
}

/// Liveness: the process answers.
async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Readiness: the dataset is loaded (the REST listener only starts once it
/// is) and no script holds the server.
async fn ready(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    if state.engine.script_running()
        && state.engine.script_state().elapsed_ms() >= state.engine.busy_threshold_ms()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "busy"})),
        );
    }
    (StatusCode::OK, Json(serde_json::json!({"status": "ready"})))
}

/// Prometheus text exposition: every numeric `INFO` field as a gauge
/// `solikv_<field>`, the keyspace per database, and per-command counters.
async fn metrics(State(state): State<AppState>) -> impl axum::response::IntoResponse {
    let engine = state.engine.clone();
    let body = tokio::task::spawn_blocking(move || render_metrics(&engine))
        .await
        .unwrap_or_default();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

pub fn render_metrics(engine: &CommandEngine) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "# TYPE solikv_up gauge\nsolikv_up 1");
    let info = match engine.execute("INFO", &[Bytes::from_static(b"default")]) {
        solikv_core::CommandResponse::BulkString(b) => String::from_utf8_lossy(&b).to_string(),
        _ => String::new(),
    };
    let mut section = String::new();
    for line in info.lines() {
        let line = line.trim();
        if let Some(name) = line.strip_prefix("# ") {
            section = name.to_ascii_lowercase();
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if section == "keyspace" {
            // db0:keys=1,expires=0,avg_ttl=0
            for part in value.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    let _ = writeln!(
                        out,
                        "solikv_keyspace_{}{{db=\"{}\"}} {}",
                        k,
                        key.trim_start_matches("db"),
                        v
                    );
                }
            }
            continue;
        }
        if value.parse::<f64>().is_ok() {
            let metric: String = key
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            let _ = writeln!(out, "solikv_{} {}", metric, value);
        }
    }
    let _ = writeln!(out, "# TYPE solikv_command_calls_total counter");
    let _ = writeln!(out, "# TYPE solikv_command_duration_seconds_total counter");
    for (name, calls, usec, rejected, failed) in engine.stats().command_totals() {
        let cmd = name.to_ascii_lowercase();
        let _ = writeln!(
            out,
            "solikv_command_calls_total{{cmd=\"{}\"}} {}",
            cmd, calls
        );
        let _ = writeln!(
            out,
            "solikv_command_duration_seconds_total{{cmd=\"{}\"}} {:.6}",
            cmd,
            usec as f64 / 1e6
        );
        let _ = writeln!(
            out,
            "solikv_command_rejected_total{{cmd=\"{}\"}} {}",
            cmd, rejected
        );
        let _ = writeln!(
            out,
            "solikv_command_failed_total{{cmd=\"{}\"}} {}",
            cmd, failed
        );
    }
    out
}

async fn auth_middleware(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    let ip = peer.ip().to_string();

    if state.auth_tracker.is_blocked(&ip) {
        tracing::warn!("REST AUTH blocked for {} (too many failures)", ip);
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({"error": "Too many failures, please wait"})),
        ));
    }

    let unauthorized = || {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Unauthorized"})),
        )
    };
    let acl = state.engine.acl();
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let user = match auth_header.as_deref() {
        // The token: `--rest-token`, else `requirepass`. It acts as
        // `--rest-token-user` (`default` unless set).
        Some(h) if h.starts_with("Bearer ") && state.password.is_some() => {
            let token = &h[7..];
            let expected = state
                .password
                .as_deref()
                .map(|p| p.as_bytes())
                .unwrap_or(b"");
            if !constant_time_eq(token.as_bytes(), expected) {
                state.auth_tracker.record_failure(&ip);
                tracing::warn!("Failed REST AUTH attempt from {}", ip);
                return Err(unauthorized());
            }
            match acl.user(&state.token_user).filter(|u| u.enabled()) {
                Some(u) => u,
                None => {
                    tracing::warn!(
                        "REST token user '{}' does not exist or is disabled",
                        state.token_user
                    );
                    return Err(unauthorized());
                }
            }
        }
        // An ACL user's own credentials.
        Some(h) if h.starts_with("Basic ") => {
            let Some((name, pass)) = decode_basic(&h[6..]) else {
                return Err(unauthorized());
            };
            match acl.authenticate(Some(name.as_bytes()), pass.as_bytes()) {
                Ok(u) => u,
                Err(_) => {
                    state.auth_tracker.record_failure(&ip);
                    acl.log_auth_failure(&name, format!("rest addr={}", peer));
                    tracing::warn!("Failed REST AUTH attempt from {} (user {})", ip, name);
                    return Err(unauthorized());
                }
            }
        }
        // No credentials (or a Bearer where no token is configured): only an
        // open server — no token, and a `default` user without a password —
        // lets the request through, as `default`. No credentials is not a
        // failed guess: counting it locked out every client behind a
        // shared address (a reverse proxy) after ten probes.
        _ => {
            if state.password.is_none() && acl.default_auto_auth() {
                acl.default_user()
            } else {
                return Err(unauthorized());
            }
        }
    };
    state.auth_tracker.record_success(&ip);
    Ok(REST_USER.scope(user, next.run(req)).await)
}

/// `Basic <base64(user:password)>`.
fn decode_basic(b64: &str) -> Option<(String, String)> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .ok()?;
    let text = String::from_utf8(raw).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Constant-time byte comparison to prevent timing side-channel attacks on password checks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---- Request/Response types ----

#[derive(Deserialize)]
struct SetKeyBody {
    value: String,
    #[serde(default)]
    ex: Option<u64>,
    #[serde(default)]
    px: Option<u64>,
}

#[derive(Deserialize)]
struct ListPushBody {
    values: Vec<String>,
}

#[derive(Deserialize)]
struct HashSetBody {
    fields: std::collections::HashMap<String, String>,
}

#[derive(Deserialize)]
struct SetAddBody {
    members: Vec<String>,
}

#[derive(Deserialize)]
struct ZSetAddBody {
    members: Vec<ZSetMember>,
}

#[derive(Deserialize)]
struct ZSetMember {
    score: f64,
    member: String,
}

#[derive(Deserialize)]
struct RangeQuery {
    start: Option<i64>,
    stop: Option<i64>,
}

#[derive(Deserialize)]
struct CommandBody {
    command: String,
    args: Vec<String>,
}

#[derive(Deserialize)]
struct PfAddBody {
    elements: Vec<String>,
}

#[derive(Deserialize)]
struct PfMergeBody {
    sources: Vec<String>,
}

#[derive(Deserialize)]
struct BfReserveBody {
    error_rate: f64,
    capacity: u64,
}

#[derive(Deserialize)]
struct BfAddBody {
    item: String,
}

#[derive(Deserialize)]
struct SetBitBody {
    offset: u64,
    value: u8,
}

#[derive(Deserialize)]
struct BitcountQuery {
    start: Option<i64>,
    end: Option<i64>,
}

#[derive(Deserialize)]
struct GeoAddBody {
    members: Vec<GeoAddMember>,
}

#[derive(Deserialize)]
struct GeoAddMember {
    longitude: f64,
    latitude: f64,
    member: String,
}

#[derive(Deserialize)]
struct GeoDistQuery {
    unit: Option<String>,
}

#[derive(Deserialize)]
struct GeoSearchBody {
    longitude: Option<f64>,
    latitude: Option<f64>,
    member: Option<String>,
    radius: Option<f64>,
    width: Option<f64>,
    height: Option<f64>,
    unit: Option<String>,
    count: Option<usize>,
    sort: Option<String>,
    withcoord: Option<bool>,
    withdist: Option<bool>,
}

// ---- Handlers ----

async fn get_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "GET", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn set_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<SetKeyBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key), Bytes::from(body.value)];
    if let Some(ex) = body.ex {
        args.push(Bytes::from("EX"));
        args.push(Bytes::from(ex.to_string()));
    } else if let Some(px) = body.px {
        args.push(Bytes::from("PX"));
        args.push(Bytes::from(px.to_string()));
    }
    let resp = exec(&state, "SET", &args);
    to_json_response(resp)
}

async fn delete_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "DEL", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn list_range(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(range): Query<RangeQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let start = range.start.unwrap_or(0);
    let stop = range.stop.unwrap_or(-1);
    let resp = exec(
        &state,
        "LRANGE",
        &[
            Bytes::from(key),
            Bytes::from(start.to_string()),
            Bytes::from(stop.to_string()),
        ],
    );
    to_json_response(resp)
}

async fn list_lpush(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ListPushBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.values.into_iter().map(Bytes::from));
    let resp = exec(&state, "LPUSH", &args);
    to_json_response(resp)
}

async fn list_rpush(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ListPushBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.values.into_iter().map(Bytes::from));
    let resp = exec(&state, "RPUSH", &args);
    to_json_response(resp)
}

async fn list_lpop(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "LPOP", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn list_rpop(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "RPOP", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn hash_getall(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "HGETALL", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn hash_get_field(
    State(state): State<AppState>,
    Path((key, field)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "HGET", &[Bytes::from(key), Bytes::from(field)]);
    to_json_response(resp)
}

async fn hash_set(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<HashSetBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    for (field, value) in body.fields {
        args.push(Bytes::from(field));
        args.push(Bytes::from(value));
    }
    let resp = exec(&state, "HSET", &args);
    to_json_response(resp)
}

async fn hash_del_field(
    State(state): State<AppState>,
    Path((key, field)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "HDEL", &[Bytes::from(key), Bytes::from(field)]);
    to_json_response(resp)
}

async fn set_members(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "SMEMBERS", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn set_add(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<SetAddBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.members.into_iter().map(Bytes::from));
    let resp = exec(&state, "SADD", &args);
    to_json_response(resp)
}

async fn set_rem_member(
    State(state): State<AppState>,
    Path((key, member)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "SREM", &[Bytes::from(key), Bytes::from(member)]);
    to_json_response(resp)
}

async fn zset_range(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(range): Query<RangeQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let start = range.start.unwrap_or(0);
    let stop = range.stop.unwrap_or(-1);
    let resp = exec(
        &state,
        "ZRANGE",
        &[
            Bytes::from(key),
            Bytes::from(start.to_string()),
            Bytes::from(stop.to_string()),
            Bytes::from("WITHSCORES"),
        ],
    );
    to_json_response(resp)
}

async fn zset_add(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ZSetAddBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    for member in body.members {
        args.push(Bytes::from(member.score.to_string()));
        args.push(Bytes::from(member.member));
    }
    let resp = exec(&state, "ZADD", &args);
    to_json_response(resp)
}

/// Commands blocked from the REST /cmd endpoint for security.
const REST_BLOCKED_COMMANDS: &[&str] = &[
    "FLUSHDB",
    "FLUSHALL",
    "SHUTDOWN",
    "DEBUG",
    "EVAL",
    "EVALSHA",
    "EVAL_RO",
    "EVALSHA_RO",
    "FCALL",
    "FCALL_RO",
    "FUNCTION",
    "SCRIPT",
    "ACL",
    "MODULE",
    "BGSAVE",
    "BGREWRITEAOF",
    "SLAVEOF",
    "REPLICAOF",
    "CLUSTER",
    "CONFIG",
    "MIGRATE",
    "RESTORE",
    "OBJECT",
    "SAVE",
    "KEYS",
    "SCAN",
    "HSCAN",
    "ZSCAN",
    "SSCAN",
    "RENAME",
    "RENAMENX",
    "RESET",
    "LATENCY",
    "MEMORY",
    "WAIT",
    "FAILOVER",
    "CLIENT",
    // INFO and DBSIZE have their own read-only routes (`/info`, `/dbsize`),
    // behind the same authentication. They are listed here so `/cmd` stays
    // data commands only, not because the answers are secret.
    "INFO",
    "DBSIZE",
];

async fn execute_command(
    State(state): State<AppState>,
    Json(body): Json<CommandBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let cmd = body.command.to_uppercase();
    if REST_BLOCKED_COMMANDS.contains(&cmd.as_str()) {
        return (
            StatusCode::FORBIDDEN,
            Json(
                serde_json::json!({"error": format!("ERR command '{}' is not allowed via REST API", cmd)}),
            ),
        );
    }
    let args: Vec<Bytes> = body.args.into_iter().map(Bytes::from).collect();
    let resp = exec(&state, &cmd, &args);
    to_json_response(resp)
}

async fn server_info(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "INFO", &[]);
    to_json_response(resp)
}

async fn dbsize(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "DBSIZE", &[]);
    to_json_response(resp)
}

// ---- HyperLogLog handlers ----

async fn pfadd(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PfAddBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.elements.into_iter().map(Bytes::from));
    let resp = exec(&state, "PFADD", &args);
    to_json_response(resp)
}

async fn pfcount(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "PFCOUNT", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn pfmerge(
    State(state): State<AppState>,
    Path(dest): Path<String>,
    Json(body): Json<PfMergeBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(dest)];
    args.extend(body.sources.into_iter().map(Bytes::from));
    let resp = exec(&state, "PFMERGE", &args);
    to_json_response(resp)
}

// ---- Bloom filter handlers ----

async fn bf_reserve(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<BfReserveBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(
        &state,
        "BF.RESERVE",
        &[
            Bytes::from(key),
            Bytes::from(body.error_rate.to_string()),
            Bytes::from(body.capacity.to_string()),
        ],
    );
    to_json_response(resp)
}

async fn bf_add(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<BfAddBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(
        &state,
        "BF.ADD",
        &[Bytes::from(key), Bytes::from(body.item)],
    );
    to_json_response(resp)
}

async fn bf_exists(
    State(state): State<AppState>,
    Path((key, item)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "BF.EXISTS", &[Bytes::from(key), Bytes::from(item)]);
    to_json_response(resp)
}

async fn bf_info(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "BF.INFO", &[Bytes::from(key)]);
    to_json_response(resp)
}

// ---- Bitmap handlers ----

async fn bitmap_setbit(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<SetBitBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(
        &state,
        "SETBIT",
        &[
            Bytes::from(key),
            Bytes::from(body.offset.to_string()),
            Bytes::from(body.value.to_string()),
        ],
    );
    to_json_response(resp)
}

async fn bitmap_getbit(
    State(state): State<AppState>,
    Path((key, offset)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "GETBIT", &[Bytes::from(key), Bytes::from(offset)]);
    to_json_response(resp)
}

async fn bitmap_bitcount(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(query): Query<BitcountQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    if let (Some(start), Some(end)) = (query.start, query.end) {
        args.push(Bytes::from(start.to_string()));
        args.push(Bytes::from(end.to_string()));
    }
    let resp = exec(&state, "BITCOUNT", &args);
    to_json_response(resp)
}

// ---- Geospatial handlers ----

async fn geo_add(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<GeoAddBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    for m in body.members {
        args.push(Bytes::from(m.longitude.to_string()));
        args.push(Bytes::from(m.latitude.to_string()));
        args.push(Bytes::from(m.member));
    }
    let resp = exec(&state, "GEOADD", &args);
    to_json_response(resp)
}

async fn geo_pos(
    State(state): State<AppState>,
    Path((key, member)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = exec(&state, "GEOPOS", &[Bytes::from(key), Bytes::from(member)]);
    to_json_response(resp)
}

async fn geo_dist(
    State(state): State<AppState>,
    Path((key, member1, member2)): Path<(String, String, String)>,
    Query(query): Query<GeoDistQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key), Bytes::from(member1), Bytes::from(member2)];
    if let Some(unit) = query.unit {
        args.push(Bytes::from(unit));
    }
    let resp = exec(&state, "GEODIST", &args);
    to_json_response(resp)
}

async fn geo_search(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<GeoSearchBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];

    // FROM
    if let (Some(lon), Some(lat)) = (body.longitude, body.latitude) {
        args.push(Bytes::from("FROMLONLAT"));
        args.push(Bytes::from(lon.to_string()));
        args.push(Bytes::from(lat.to_string()));
    } else if let Some(member) = body.member {
        args.push(Bytes::from("FROMMEMBER"));
        args.push(Bytes::from(member));
    }

    let unit = body.unit.unwrap_or_else(|| "m".to_string());

    // BY
    if let Some(radius) = body.radius {
        args.push(Bytes::from("BYRADIUS"));
        args.push(Bytes::from(radius.to_string()));
        args.push(Bytes::from(unit.clone()));
    } else if let (Some(w), Some(h)) = (body.width, body.height) {
        args.push(Bytes::from("BYBOX"));
        args.push(Bytes::from(w.to_string()));
        args.push(Bytes::from(h.to_string()));
        args.push(Bytes::from(unit.clone()));
    }

    // Sort
    if let Some(sort) = body.sort {
        args.push(Bytes::from(sort.to_uppercase()));
    }

    // Count
    if let Some(count) = body.count {
        args.push(Bytes::from("COUNT"));
        args.push(Bytes::from(count.to_string()));
    }

    // Flags
    if body.withcoord.unwrap_or(false) {
        args.push(Bytes::from("WITHCOORD"));
    }
    if body.withdist.unwrap_or(false) {
        args.push(Bytes::from("WITHDIST"));
    }

    let resp = exec(&state, "GEOSEARCH", &args);
    to_json_response(resp)
}

fn to_json_response(resp: CommandResponse) -> (StatusCode, Json<serde_json::Value>) {
    let status = match &resp {
        CommandResponse::Error(e) if e.starts_with("NOPERM") => StatusCode::FORBIDDEN,
        CommandResponse::Error(_) => StatusCode::BAD_REQUEST,
        CommandResponse::Nil => StatusCode::NOT_FOUND,
        _ => StatusCode::OK,
    };
    (status, Json(response_to_json(&resp)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_names_are_extracted_without_port() {
        assert_eq!(host_name("localhost:5020"), "localhost");
        assert_eq!(host_name("127.0.0.1"), "127.0.0.1");
        assert_eq!(host_name("[::1]:5020"), "::1");
        assert_eq!(host_name("[::1]"), "::1");
        assert_eq!(host_name("evil.example:5020"), "evil.example");
    }

    #[test]
    fn the_auth_failure_map_is_pruned() {
        let tracker = AuthFailureTracker::default();
        {
            let mut map = tracker.failures.write().unwrap();
            // `Instant` cannot predate boot; on a machine up for less than
            // the cooldown there is no "long ago" to test with.
            let Some(long_ago) = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(AUTH_COOLDOWN_SECS_REST + 5))
            else {
                return;
            };
            for i in 0..AUTH_TRACKER_PRUNE_AT {
                map.insert(format!("10.0.{}.{}", i / 256, i % 256), (1, long_ago));
            }
        }
        tracker.record_failure("192.0.2.1");
        assert_eq!(tracker.failures.read().unwrap().len(), 1);
    }
}
