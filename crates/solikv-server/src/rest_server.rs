use axum::{
    extract::{Path, Query, Request, State},
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
use std::sync::Arc;

use solikv_core::CommandResponse;
use solikv_engine::response::response_to_json;
use solikv_engine::CommandEngine;

#[derive(Clone)]
struct AppState {
    engine: Arc<CommandEngine>,
    password: Option<Arc<String>>,
}

pub async fn run(
    addr: &str,
    engine: Arc<CommandEngine>,
    password: Option<Arc<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { engine, password };

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
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("REST API listening on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<axum::response::Response, (StatusCode, Json<serde_json::Value>)> {
    let password = match &state.password {
        None => return Ok(next.run(req).await),
        Some(p) => p,
    };

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header[7..];
            if token == password.as_str() {
                Ok(next.run(req).await)
            } else {
                Err((
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({"error": "Unauthorized"})),
                ))
            }
        }
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Unauthorized"})),
        )),
    }
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
    let resp = state.engine.execute(
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
    let resp = state
        .engine
        .execute("HGET", &[Bytes::from(key), Bytes::from(field)]);
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
    let resp = state
        .engine
        .execute("HDEL", &[Bytes::from(key), Bytes::from(field)]);
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
    let resp = state
        .engine
        .execute("SREM", &[Bytes::from(key), Bytes::from(member)]);
    to_json_response(resp)
}

async fn zset_range(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(range): Query<RangeQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let start = range.start.unwrap_or(0);
    let stop = range.stop.unwrap_or(-1);
    let resp = state.engine.execute(
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

async fn server_info(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("INFO", &[]);
    to_json_response(resp)
}

async fn dbsize(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("DBSIZE", &[]);
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
    let resp = state.engine.execute("PFADD", &args);
    to_json_response(resp)
}

async fn pfcount(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("PFCOUNT", &[Bytes::from(key)]);
    to_json_response(resp)
}

async fn pfmerge(
    State(state): State<AppState>,
    Path(dest): Path<String>,
    Json(body): Json<PfMergeBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut args = vec![Bytes::from(dest)];
    args.extend(body.sources.into_iter().map(Bytes::from));
    let resp = state.engine.execute("PFMERGE", &args);
    to_json_response(resp)
}

// ---- Bloom filter handlers ----

async fn bf_reserve(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<BfReserveBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute(
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
    let resp = state
        .engine
        .execute("BF.ADD", &[Bytes::from(key), Bytes::from(body.item)]);
    to_json_response(resp)
}

async fn bf_exists(
    State(state): State<AppState>,
    Path((key, item)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state
        .engine
        .execute("BF.EXISTS", &[Bytes::from(key), Bytes::from(item)]);
    to_json_response(resp)
}

async fn bf_info(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute("BF.INFO", &[Bytes::from(key)]);
    to_json_response(resp)
}

// ---- Bitmap handlers ----

async fn bitmap_setbit(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<SetBitBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state.engine.execute(
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
    let resp = state
        .engine
        .execute("GETBIT", &[Bytes::from(key), Bytes::from(offset)]);
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
    let resp = state.engine.execute("BITCOUNT", &args);
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
    let resp = state.engine.execute("GEOADD", &args);
    to_json_response(resp)
}

async fn geo_pos(
    State(state): State<AppState>,
    Path((key, member)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    let resp = state
        .engine
        .execute("GEOPOS", &[Bytes::from(key), Bytes::from(member)]);
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
    let resp = state.engine.execute("GEODIST", &args);
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

    let resp = state.engine.execute("GEOSEARCH", &args);
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
