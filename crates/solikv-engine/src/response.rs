use solikv_core::CommandResponse;

/// Convert a CommandResponse to a JSON value for REST API.
pub fn response_to_json(resp: &CommandResponse) -> serde_json::Value {
    match resp {
        CommandResponse::Ok => serde_json::json!({"result": "OK"}),
        CommandResponse::Nil | CommandResponse::NilArray => serde_json::Value::Null,
        CommandResponse::Integer(n) => serde_json::json!(n),
        CommandResponse::BulkString(b) => match std::str::from_utf8(b) {
            Ok(s) => serde_json::json!(s),
            Err(_) => serde_json::json!(format!("<binary:{} bytes>", b.len())),
        },
        CommandResponse::SimpleString(s) => match std::str::from_utf8(s) {
            Ok(s) => serde_json::json!(s),
            Err(_) => serde_json::json!("OK"),
        },
        CommandResponse::Array(items) => {
            let arr: Vec<serde_json::Value> = items.iter().map(response_to_json).collect();
            serde_json::json!(arr)
        }
        CommandResponse::Error(msg) => serde_json::json!({"error": msg}),
        CommandResponse::Queued => serde_json::json!("QUEUED"),
        CommandResponse::Double(d) => serde_json::json!(d),
        CommandResponse::Bool(b) => serde_json::json!(b),
        CommandResponse::Map(pairs) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in pairs {
                let key = match response_to_json(k) {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                obj.insert(key, response_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        CommandResponse::Set(items) | CommandResponse::Push(items) => {
            serde_json::json!(items.iter().map(response_to_json).collect::<Vec<_>>())
        }
        CommandResponse::Pairs(pairs) => serde_json::json!(pairs
            .iter()
            .map(|(a, b)| serde_json::json!([response_to_json(a), response_to_json(b)]))
            .collect::<Vec<_>>()),
        CommandResponse::Verbatim(_, b) => serde_json::json!(String::from_utf8_lossy(b)),
    }
}
