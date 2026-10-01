//! RESP3 reply types.
//!
//! Commands build their replies in RESP2 shapes — a hash is a flat array of
//! field, value, …; a score a bulk string. A client that switched to RESP3
//! (`HELLO 3`) expects the same data typed: a map, a double, a set. Rather
//! than make every command produce two shapes, [`shape`] re-types a finished
//! reply by the command that produced it, only for RESP3 connections. RESP2
//! output stays byte-for-byte what it was, and costs nothing more.

use bytes::Bytes;
use solikv_core::CommandResponse;

fn eq(a: &Bytes, b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}

fn has_opt(args: &[Bytes], opt: &str) -> bool {
    args.iter().any(|a| eq(a, opt))
}

/// A bulk-string score as a double; anything else unchanged.
fn double(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::BulkString(b) => {
            match std::str::from_utf8(&b).ok().and_then(parse_double) {
                Some(d) => CommandResponse::Double(d),
                None => CommandResponse::BulkString(b),
            }
        }
        other => other,
    }
}

fn parse_double(s: &str) -> Option<f64> {
    match s {
        "inf" | "+inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        _ => s.parse().ok(),
    }
}

/// A flat `[k, v, k, v, …]` array as a map.
fn map(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::Array(items) if items.len() % 2 == 0 => {
            let mut it = items.into_iter();
            let mut pairs = Vec::new();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                pairs.push((k, v));
            }
            CommandResponse::Map(pairs)
        }
        other => other,
    }
}

/// A flat `[member, score, …]` array as pairs with double scores.
fn scored_pairs(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::Array(items) if items.len() % 2 == 0 => {
            let mut it = items.into_iter();
            let mut pairs = Vec::new();
            while let (Some(m), Some(s)) = (it.next(), it.next()) {
                pairs.push((m, double(s)));
            }
            CommandResponse::Pairs(pairs)
        }
        other => other,
    }
}

/// A flat `[field, value, …]` array as pairs (values untyped).
fn pairs(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::Array(items) if items.len() % 2 == 0 => {
            let mut it = items.into_iter();
            let mut out = Vec::new();
            while let (Some(a), Some(b)) = (it.next(), it.next()) {
                out.push((a, b));
            }
            CommandResponse::Pairs(out)
        }
        other => other,
    }
}

fn set(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::Array(items) => CommandResponse::Set(items),
        other => other,
    }
}

fn verbatim(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::BulkString(b) => CommandResponse::Verbatim("txt", b),
        other => other,
    }
}

/// `XREAD`/`XREADGROUP`: `[[key, entries], …]` → `{key: entries}`.
fn stream_map(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::Array(items) => {
            let mut pairs = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    CommandResponse::Array(mut kv) if kv.len() == 2 => {
                        let v = kv.pop().unwrap_or(CommandResponse::Nil);
                        let k = kv.pop().unwrap_or(CommandResponse::Nil);
                        pairs.push((k, v));
                    }
                    other => return CommandResponse::Array(vec![other]),
                }
            }
            CommandResponse::Map(pairs)
        }
        other => other,
    }
}

/// Maps nested one level down (`XINFO GROUPS`: an array of maps).
fn array_of_maps(r: CommandResponse) -> CommandResponse {
    match r {
        CommandResponse::Array(items) => {
            CommandResponse::Array(items.into_iter().map(map).collect())
        }
        other => other,
    }
}

/// Re-type `resp`, the RESP2-shaped reply of `name args…`, for RESP3.
pub fn shape(name: &str, args: &[Bytes], resp: CommandResponse) -> CommandResponse {
    if resp.is_error() {
        return resp;
    }
    match name {
        "HGETALL" => map(resp),
        "SMEMBERS" | "SINTER" | "SUNION" | "SDIFF" => set(resp),
        "ZSCORE" | "ZINCRBY" => double(resp),
        "ZADD" if has_opt(args, "INCR") => double(resp),
        "ZMSCORE" => match resp {
            CommandResponse::Array(items) => {
                CommandResponse::Array(items.into_iter().map(double).collect())
            }
            other => other,
        },
        "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZUNION" | "ZINTER"
        | "ZDIFF" | "ZRANDMEMBER"
            if has_opt(args, "WITHSCORES") =>
        {
            scored_pairs(resp)
        }
        "ZPOPMIN" | "ZPOPMAX" => {
            if args.len() > 1 {
                scored_pairs(resp)
            } else {
                match resp {
                    CommandResponse::Array(mut items) if items.len() == 2 => {
                        let s = items.pop().map(double).unwrap_or(CommandResponse::Nil);
                        items.push(s);
                        CommandResponse::Array(items)
                    }
                    other => other,
                }
            }
        }
        "BZPOPMIN" | "BZPOPMAX" => match resp {
            CommandResponse::Array(mut items) if items.len() == 3 => {
                let s = items.pop().map(double).unwrap_or(CommandResponse::Nil);
                items.push(s);
                CommandResponse::Array(items)
            }
            other => other,
        },
        "ZMPOP" | "BZMPOP" => match resp {
            CommandResponse::Array(mut items) if items.len() == 2 => {
                let elems = items
                    .pop()
                    .map(scored_pairs)
                    .unwrap_or(CommandResponse::Nil);
                items.push(elems);
                CommandResponse::Array(items)
            }
            other => other,
        },
        "HRANDFIELD" if has_opt(args, "WITHVALUES") => pairs(resp),
        "XREAD" | "XREADGROUP" => stream_map(resp),
        "XINFO" => match args.first() {
            Some(sub) if eq(sub, "STREAM") => map(resp),
            Some(sub) if eq(sub, "GROUPS") || eq(sub, "CONSUMERS") => array_of_maps(resp),
            _ => resp,
        },
        "CONFIG" if args.first().is_some_and(|s| eq(s, "GET")) => map(resp),
        "MEMORY" if args.first().is_some_and(|s| eq(s, "STATS")) => map(resp),
        "ACL" if args.first().is_some_and(|s| eq(s, "GETUSER")) => map(resp),
        "CLIENT" if args.first().is_some_and(|s| eq(s, "TRACKINGINFO")) => map(resp),
        "CLIENT" if args.first().is_some_and(|s| eq(s, "INFO") || eq(s, "LIST")) => verbatim(resp),
        "INFO" => verbatim(resp),
        "LATENCY" | "MEMORY" if args.first().is_some_and(|s| eq(s, "DOCTOR")) => verbatim(resp),
        "FUNCTION" if args.first().is_some_and(|s| eq(s, "LIST")) => array_of_maps(resp),
        "FUNCTION" if args.first().is_some_and(|s| eq(s, "STATS")) => map(resp),
        "COMMAND" if args.first().is_some_and(|s| eq(s, "DOCS")) => map(resp),
        _ => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> CommandResponse {
        CommandResponse::BulkString(Bytes::from(s.to_string()))
    }

    #[test]
    fn hashes_become_maps_and_scores_doubles() {
        let r = shape("HGETALL", &[], CommandResponse::Array(vec![b("f"), b("v")]));
        assert!(matches!(r, CommandResponse::Map(ref p) if p.len() == 1));
        let r = shape("ZSCORE", &[], b("1.5"));
        assert!(matches!(r, CommandResponse::Double(d) if d == 1.5));
        let r = shape(
            "ZRANGE",
            &[
                Bytes::from("z"),
                Bytes::from("0"),
                Bytes::from("-1"),
                Bytes::from("withscores"),
            ],
            CommandResponse::Array(vec![b("a"), b("1"), b("b"), b("2")]),
        );
        assert!(matches!(r, CommandResponse::Pairs(ref p) if p.len() == 2));
        let r = shape("SMEMBERS", &[], CommandResponse::Array(vec![b("a")]));
        assert!(matches!(r, CommandResponse::Set(_)));
        let r = shape("GET", &[], b("x"));
        assert!(matches!(r, CommandResponse::BulkString(_)));
    }
}
