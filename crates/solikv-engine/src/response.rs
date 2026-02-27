use solikv_core::CommandResponse;

/// Convert a CommandResponse to a JSON value for REST API.
pub fn response_to_json(resp: &CommandResponse) -> serde_json::Value {
    match resp {
        CommandResponse::Ok => serde_json::json!({"result": "OK"}),
        CommandResponse::Nil => serde_json::Value::Null,
        CommandResponse::Integer(n) => serde_json::json!(n),
        CommandResponse::BulkString(b) => {
            match std::str::from_utf8(b) {
                Ok(s) => serde_json::json!(s),
                Err(_) => serde_json::json!(format!("<binary:{} bytes>", b.len())),
            }
        }
        CommandResponse::SimpleString(s) => {
            match std::str::from_utf8(s) {
                Ok(s) => serde_json::json!(s),
                Err(_) => serde_json::json!("OK"),
            }
        }
        CommandResponse::Array(items) => {
            let arr: Vec<serde_json::Value> = items.iter().map(response_to_json).collect();
            serde_json::json!(arr)
        }
        CommandResponse::Error(msg) => serde_json::json!({"error": msg}),
        CommandResponse::Queued => serde_json::json!("QUEUED"),
    }
}
