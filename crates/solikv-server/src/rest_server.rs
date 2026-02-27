use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post, put},
    Router,
};
use bytes::Bytes;
use serde::Deserialize;
#[allow(unused_imports)]
use serde::Serialize;
use std::sync::Arc;

use solikv_core::CommandResponse;
use solikv_engine::response::response_to_json;
use solikv_engine::CommandEngine;

#[derive(Clone)]
struct AppState {
    engine: Arc<CommandEngine>,
}

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { engine };

    let app = Router::new()
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
        // Generic command endpoint
        .route("/cmd", post(execute_command))
        // Server info
        .route("/info", get(server_info))
        .route("/dbsize", get(dbsize))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("REST API listening on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
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

// ---- Handlers ----

async fn get_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("GET", &[Bytes::from(key)]);
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
    let resp = state.engine.execute("SET", &args);
    to_json_response(resp)
}

async fn delete_key(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("DEL", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn list_range(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(range): Query<RangeQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let start = range.start.unwrap_or(0);
    let stop = range.stop.unwrap_or(-1);
    let resp = state.engine.execute("LRANGE", &[
        Bytes::from(key),
        Bytes::from(start.to_string()),
        Bytes::from(stop.to_string()),
    ]);
    to_json_response(resp)
}

async fn list_lpush(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ListPushBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.values.into_iter().map(Bytes::from));
    let resp = state.engine.execute("LPUSH", &args);
    to_json_response(resp)
}

async fn list_rpush(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<ListPushBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.values.into_iter().map(Bytes::from));
    let resp = state.engine.execute("RPUSH", &args);
    to_json_response(resp)
}

async fn list_lpop(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("LPOP", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn list_rpop(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("RPOP", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn hash_getall(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("HGETALL", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn hash_get_field(
    State(state): State<AppState>,
    Path((key, field)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("HGET", &[Bytes::from(key), Bytes::from(field)]);
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
    let resp = state.engine.execute("HSET", &args);
    to_json_response(resp)
}

async fn hash_del_field(
    State(state): State<AppState>,
    Path((key, field)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("HDEL", &[Bytes::from(key), Bytes::from(field)]);
    to_json_response(resp)
}

async fn set_members(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("SMEMBERS", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn set_add(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<SetAddBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(key)];
    args.extend(body.members.into_iter().map(Bytes::from));
    let resp = state.engine.execute("SADD", &args);
    to_json_response(resp)
}

async fn set_rem_member(
    State(state): State<AppState>,
    Path((key, member)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("SREM", &[Bytes::from(key), Bytes::from(member)]);
    to_json_response(resp)
}

async fn zset_range(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(range): Query<RangeQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let start = range.start.unwrap_or(0);
    let stop = range.stop.unwrap_or(-1);
    let resp = state.engine.execute("ZRANGE", &[
        Bytes::from(key),
        Bytes::from(start.to_string()),
        Bytes::from(stop.to_string()),
        Bytes::from("WITHSCORES"),
    ]);
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
    let resp = state.engine.execute("ZADD", &args);
    to_json_response(resp)
}

async fn execute_command(
    State(state): State<AppState>,
    Json(body): Json<CommandBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let args: Vec<Bytes> = body.args.into_iter().map(Bytes::from).collect();
    let resp = state.engine.execute(&body.command.to_uppercase(), &args);
    to_json_response(resp)
}

async fn server_info(
    State(state): State<AppState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("INFO", &[]);
    to_json_response(resp)
}

async fn dbsize(
    State(state): State<AppState>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("DBSIZE", &[]);
    to_json_response(resp)
}

fn to_json_response(resp: CommandResponse) -> (StatusCode, Json<serde_json::Value>) {
    let status = match &resp {
        CommandResponse::Error(_) => StatusCode::BAD_REQUEST,
        CommandResponse::Nil => StatusCode::NOT_FOUND,
        _ => StatusCode::OK,
    };
    (status, Json(response_to_json(&resp)))
}
