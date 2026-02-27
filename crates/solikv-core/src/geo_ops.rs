use bytes::Bytes;

use crate::store::ShardStore;
use crate::types::*;

// ── Constants ──

const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LON_MIN: f64 = -180.0;
const GEO_LON_MAX: f64 = 180.0;
const GEO_STEP: u32 = 26; // 26 bits per axis, 52-bit geohash total
const EARTH_RADIUS_M: f64 = 6372797.560856;

// ── Geohash encoding/decoding ──

/// Encode longitude/latitude into a 52-bit geohash (26 bits per axis, interleaved).
pub fn geo_encode(lon: f64, lat: f64) -> u64 {
    let lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let lon_offset = (lon - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);

    let lat_bits = (lat_offset * ((1u64 << GEO_STEP) as f64)) as u64;
    let lon_bits = (lon_offset * ((1u64 << GEO_STEP) as f64)) as u64;

    interleave(lon_bits, lat_bits)
}

/// Decode a 52-bit geohash back to (longitude, latitude) center.
pub fn geo_decode(hash: u64) -> (f64, f64) {
    let (lon_bits, lat_bits) = deinterleave(hash);

    let lon_min = GEO_LON_MIN + (lon_bits as f64 / (1u64 << GEO_STEP) as f64) * (GEO_LON_MAX - GEO_LON_MIN);
    let lon_max = GEO_LON_MIN + ((lon_bits + 1) as f64 / (1u64 << GEO_STEP) as f64) * (GEO_LON_MAX - GEO_LON_MIN);
    let lat_min = GEO_LAT_MIN + (lat_bits as f64 / (1u64 << GEO_STEP) as f64) * (GEO_LAT_MAX - GEO_LAT_MIN);
    let lat_max = GEO_LAT_MIN + ((lat_bits + 1) as f64 / (1u64 << GEO_STEP) as f64) * (GEO_LAT_MAX - GEO_LAT_MIN);

    ((lon_min + lon_max) / 2.0, (lat_min + lat_max) / 2.0)
}

/// Produce an 11-character base32 geohash string (like Redis GEOHASH command).
/// Uses standard geohash algorithm: alternating longitude/latitude bit subdivision.
pub fn geo_hash_string(lon: f64, lat: f64) -> String {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

    // We need 55 bits for 11 base32 chars (5 bits each).
    // Standard geohash uses -90/+90 lat and -180/+180 lon bounds (not Redis internal bounds).
    let mut lat_min = -90.0_f64;
    let mut lat_max = 90.0_f64;
    let mut lon_min = -180.0_f64;
    let mut lon_max = 180.0_f64;

    let mut result = String::with_capacity(11);
    let mut bits: u8 = 0;
    let mut bit_count: u8 = 0;
    let mut is_lon = true;

    for _ in 0..55 {
        if is_lon {
            let mid = (lon_min + lon_max) / 2.0;
            if lon >= mid {
                bits = (bits << 1) | 1;
                lon_min = mid;
            } else {
                bits <<= 1;
                lon_max = mid;
            }
        } else {
            let mid = (lat_min + lat_max) / 2.0;
            if lat >= mid {
                bits = (bits << 1) | 1;
                lat_min = mid;
            } else {
                bits <<= 1;
                lat_max = mid;
            }
        }
        is_lon = !is_lon;
        bit_count += 1;

        if bit_count == 5 {
            result.push(BASE32[bits as usize] as char);
            bits = 0;
            bit_count = 0;
        }
    }

    result
}

/// Haversine distance between two points, in meters.
pub fn geo_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

/// Convert meters to the given unit.
pub fn convert_distance(meters: f64, unit: &str) -> f64 {
    match unit {
        "km" => meters / 1000.0,
        "ft" => meters * 3.28084,
        "mi" => meters / 1609.344,
        _ => meters, // "m"
    }
}

/// Convert a distance in the given unit to meters.
pub fn unit_to_meters(dist: f64, unit: &str) -> f64 {
    match unit {
        "km" => dist * 1000.0,
        "ft" => dist / 3.28084,
        "mi" => dist * 1609.344,
        _ => dist,
    }
}

// ── Bit interleaving helpers ──

fn interleave(x: u64, y: u64) -> u64 {
    let mut result: u64 = 0;
    for i in 0..GEO_STEP {
        result |= ((x >> i) & 1) << (2 * i + 1);
        result |= ((y >> i) & 1) << (2 * i);
    }
    result
}

fn deinterleave(hash: u64) -> (u64, u64) {
    let mut x: u64 = 0;
    let mut y: u64 = 0;
    for i in 0..GEO_STEP {
        x |= ((hash >> (2 * i + 1)) & 1) << i;
        y |= ((hash >> (2 * i)) & 1) << i;
    }
    (x, y)
}

// ── GEOSEARCH types ──

#[derive(Debug, Clone)]
pub enum GeoFrom {
    Member(Bytes),
    LonLat(f64, f64),
}

#[derive(Debug, Clone)]
pub enum GeoBy {
    Radius(f64, String),        // distance, unit
    Box(f64, f64, String),      // width, height, unit
}

// ── ShardStore methods ──

impl ShardStore {
    /// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
    pub fn geo_add(
        &mut self,
        key: &Bytes,
        nx: bool,
        xx: bool,
        ch: bool,
        items: &[(f64, f64, Bytes)],
    ) -> CommandResponse {
        // Validate coordinates
        for &(lon, lat, _) in items {
            if lon < GEO_LON_MIN || lon > GEO_LON_MAX || lat < GEO_LAT_MIN || lat > GEO_LAT_MAX {
                return CommandResponse::error(format!(
                    "ERR invalid longitude,latitude pair {:.6},{:.6}",
                    lon, lat
                ));
            }
        }

        // Build score-member pairs from geohash
        let pairs: Vec<(f64, Bytes)> = items
            .iter()
            .map(|(lon, lat, member)| {
                let hash = geo_encode(*lon, *lat);
                (hash as f64, member.clone())
            })
            .collect();

        // Delegate to existing ZADD logic
        self.zset_zadd(key, pairs, nx, xx, false, false, ch)
    }

    /// GEOPOS key member [member ...]
    pub fn geo_pos(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let zset = match self.get(key) {
            None => return CommandResponse::array(members.iter().map(|_| CommandResponse::nil()).collect()),
            Some(entry) => match &entry.value {
                RedisValue::ZSet(zs) => zs,
                _ => return CommandResponse::wrong_type(),
            },
        };

        let result: Vec<CommandResponse> = members
            .iter()
            .map(|member| {
                match zset.score(member) {
                    None => CommandResponse::nil(),
                    Some(score) => {
                        let (lon, lat) = geo_decode(score as u64);
                        CommandResponse::array(vec![
                            CommandResponse::bulk(Bytes::from(format!("{:.17}", lon))),
                            CommandResponse::bulk(Bytes::from(format!("{:.17}", lat))),
                        ])
                    }
                }
            })
            .collect();

        CommandResponse::array(result)
    }

    /// GEOHASH key member [member ...]
    pub fn geo_hash(&mut self, key: &Bytes, members: &[Bytes]) -> CommandResponse {
        let zset = match self.get(key) {
            None => return CommandResponse::array(members.iter().map(|_| CommandResponse::nil()).collect()),
            Some(entry) => match &entry.value {
                RedisValue::ZSet(zs) => zs,
                _ => return CommandResponse::wrong_type(),
            },
        };

        let result: Vec<CommandResponse> = members
            .iter()
            .map(|member| {
                match zset.score(member) {
                    None => CommandResponse::nil(),
                    Some(score) => {
                        let (lon, lat) = geo_decode(score as u64);
                        let hash_str = geo_hash_string(lon, lat);
                        CommandResponse::bulk(Bytes::from(hash_str))
                    }
                }
            })
            .collect();

        CommandResponse::array(result)
    }

    /// GEODIST key member1 member2 [m|km|ft|mi]
    pub fn geo_dist(
        &mut self,
        key: &Bytes,
        member1: &Bytes,
        member2: &Bytes,
        unit: &str,
    ) -> CommandResponse {
        let zset = match self.get(key) {
            None => return CommandResponse::nil(),
            Some(entry) => match &entry.value {
                RedisValue::ZSet(zs) => zs,
                _ => return CommandResponse::wrong_type(),
            },
        };

        let score1 = match zset.score(member1) {
            Some(s) => s,
            None => return CommandResponse::nil(),
        };
        let score2 = match zset.score(member2) {
            Some(s) => s,
            None => return CommandResponse::nil(),
        };

        let (lon1, lat1) = geo_decode(score1 as u64);
        let (lon2, lat2) = geo_decode(score2 as u64);
        let dist = geo_distance(lon1, lat1, lon2, lat2);
        let converted = convert_distance(dist, unit);

        CommandResponse::bulk(Bytes::from(format!("{:.4}", converted)))
    }

    /// GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat
    ///   BYRADIUS radius unit | BYBOX width height unit
    ///   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
    pub fn geo_search(
        &mut self,
        key: &Bytes,
        from: &GeoFrom,
        by: &GeoBy,
        sort_asc: Option<bool>,
        count: Option<usize>,
        withcoord: bool,
        withdist: bool,
        withhash: bool,
    ) -> CommandResponse {
        let zset = match self.get(key) {
            None => return CommandResponse::array(vec![]),
            Some(entry) => match &entry.value {
                RedisValue::ZSet(zs) => zs,
                _ => return CommandResponse::wrong_type(),
            },
        };

        // Resolve center
        let (center_lon, center_lat) = match from {
            GeoFrom::LonLat(lon, lat) => (*lon, *lat),
            GeoFrom::Member(member) => {
                match zset.score(member) {
                    Some(score) => geo_decode(score as u64),
                    None => return CommandResponse::array(vec![]),
                }
            }
        };

        // Collect and filter candidates
        let mut candidates: Vec<(f64, f64, f64, Bytes, u64)> = Vec::new(); // (dist, lon, lat, member, hash)

        for (member, &score) in &zset.members {
            let hash = score.into_inner() as u64;
            let (lon, lat) = geo_decode(hash);

            match by {
                GeoBy::Radius(radius, unit) => {
                    let radius_m = unit_to_meters(*radius, unit);
                    let dist = geo_distance(center_lon, center_lat, lon, lat);
                    if dist <= radius_m {
                        candidates.push((dist, lon, lat, member.clone(), hash));
                    }
                }
                GeoBy::Box(width, height, unit) => {
                    let half_w_m = unit_to_meters(*width / 2.0, unit);
                    let half_h_m = unit_to_meters(*height / 2.0, unit);
                    // Use simple lat/lon distance for box check
                    let dlat_m = geo_distance(center_lon, center_lat, center_lon, lat);
                    let dlon_m = geo_distance(center_lon, center_lat, lon, center_lat);
                    if dlat_m <= half_h_m && dlon_m <= half_w_m {
                        let dist = geo_distance(center_lon, center_lat, lon, lat);
                        candidates.push((dist, lon, lat, member.clone(), hash));
                    }
                }
            }
        }

        // Sort
        match sort_asc {
            Some(true) => candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)),
            Some(false) => candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)),
            None => {} // no sorting specified
        }

        // Apply count limit
        if let Some(c) = count {
            candidates.truncate(c);
        }

        // Build response
        let unit_str = match by {
            GeoBy::Radius(_, u) => u.as_str(),
            GeoBy::Box(_, _, u) => u.as_str(),
        };

        let result: Vec<CommandResponse> = candidates
            .iter()
            .map(|(dist, lon, lat, member, hash)| {
                if !withcoord && !withdist && !withhash {
                    return CommandResponse::bulk(member.clone());
                }
                let mut items = vec![CommandResponse::bulk(member.clone())];
                if withdist {
                    let d = convert_distance(*dist, unit_str);
                    items.push(CommandResponse::bulk(Bytes::from(format!("{:.4}", d))));
                }
                if withhash {
                    items.push(CommandResponse::integer(*hash as i64));
                }
                if withcoord {
                    items.push(CommandResponse::array(vec![
                        CommandResponse::bulk(Bytes::from(format!("{:.17}", lon))),
                        CommandResponse::bulk(Bytes::from(format!("{:.17}", lat))),
                    ]));
                }
                CommandResponse::array(items)
            })
            .collect();

        CommandResponse::array(result)
    }

    /// GEOSEARCHSTORE dest src FROMMEMBER/FROMLONLAT ... BYRADIUS/BYBOX ... [STOREDIST]
    pub fn geo_search_store(
        &mut self,
        dest: &Bytes,
        src_key: &Bytes,
        from: &GeoFrom,
        by: &GeoBy,
        sort_asc: Option<bool>,
        count: Option<usize>,
        store_dist: bool,
    ) -> CommandResponse {
        // First, perform the search on src
        let zset = match self.get(src_key) {
            None => {
                // Empty source -> delete dest, return 0
                self.del(dest);
                return CommandResponse::integer(0);
            }
            Some(entry) => match &entry.value {
                RedisValue::ZSet(zs) => zs.clone(),
                _ => return CommandResponse::wrong_type(),
            },
        };

        // Resolve center
        let (center_lon, center_lat) = match from {
            GeoFrom::LonLat(lon, lat) => (*lon, *lat),
            GeoFrom::Member(member) => {
                match zset.score(member) {
                    Some(score) => geo_decode(score as u64),
                    None => {
                        self.del(dest);
                        return CommandResponse::integer(0);
                    }
                }
            }
        };

        let unit_str = match by {
            GeoBy::Radius(_, u) => u.as_str(),
            GeoBy::Box(_, _, u) => u.as_str(),
        };

        // Collect candidates
        let mut candidates: Vec<(f64, Bytes, u64)> = Vec::new(); // (dist, member, hash)

        for (member, &score) in &zset.members {
            let hash = score.into_inner() as u64;
            let (lon, lat) = geo_decode(hash);

            match by {
                GeoBy::Radius(radius, unit) => {
                    let radius_m = unit_to_meters(*radius, unit);
                    let dist = geo_distance(center_lon, center_lat, lon, lat);
                    if dist <= radius_m {
                        candidates.push((dist, member.clone(), hash));
                    }
                }
                GeoBy::Box(width, height, unit) => {
                    let half_w_m = unit_to_meters(*width / 2.0, unit);
                    let half_h_m = unit_to_meters(*height / 2.0, unit);
                    let dlat_m = geo_distance(center_lon, center_lat, center_lon, lat);
                    let dlon_m = geo_distance(center_lon, center_lat, lon, center_lat);
                    if dlat_m <= half_h_m && dlon_m <= half_w_m {
                        let dist = geo_distance(center_lon, center_lat, lon, lat);
                        candidates.push((dist, member.clone(), hash));
                    }
                }
            }
        }

        // Sort
        match sort_asc {
            Some(true) => candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)),
            Some(false) => candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)),
            None => {}
        }

        if let Some(c) = count {
            candidates.truncate(c);
        }

        // Store into dest as a ZSet
        let mut dest_zset = ZSetValue::new();
        for (dist, member, hash) in &candidates {
            let score = if store_dist {
                convert_distance(*dist, unit_str)
            } else {
                *hash as f64
            };
            dest_zset.insert(score, member.clone());
        }

        let len = dest_zset.len() as i64;
        if dest_zset.is_empty() {
            self.del(dest);
        } else {
            self.set(dest.clone(), RedisValue::ZSet(dest_zset), None);
        }

        CommandResponse::integer(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_encode_decode() {
        // Palermo: 13.361389, 38.115556
        let hash = geo_encode(13.361389, 38.115556);
        let (lon, lat) = geo_decode(hash);
        assert!((lon - 13.361389).abs() < 0.001, "lon: {}", lon);
        assert!((lat - 38.115556).abs() < 0.001, "lat: {}", lat);
    }

    #[test]
    fn test_geo_encode_decode_negative() {
        // New York: -73.935242, 40.730610
        let hash = geo_encode(-73.935242, 40.730610);
        let (lon, lat) = geo_decode(hash);
        assert!((lon - (-73.935242)).abs() < 0.001, "lon: {}", lon);
        assert!((lat - 40.730610).abs() < 0.001, "lat: {}", lat);
    }

    #[test]
    fn test_geo_hash_string() {
        let hash = geo_hash_string(13.361389, 38.115556);
        assert_eq!(hash.len(), 11);
        // Redis produces "sqc8b49rny0" for Palermo — verify prefix matches
        assert!(hash.starts_with("sqc8b49"), "hash: {}", hash);
    }

    #[test]
    fn test_geo_distance() {
        // Palermo to Catania ~166 km
        let dist = geo_distance(13.361389, 38.115556, 15.087269, 37.502669);
        let km = dist / 1000.0;
        assert!((km - 166.274).abs() < 1.0, "dist_km: {}", km);
    }

    #[test]
    fn test_convert_distance() {
        assert!((convert_distance(1000.0, "km") - 1.0).abs() < 0.001);
        assert!((convert_distance(1609.344, "mi") - 1.0).abs() < 0.001);
        assert!((convert_distance(1.0, "ft") - 3.28084).abs() < 0.01);
        assert!((convert_distance(100.0, "m") - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_geo_add_and_pos() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mygeo");

        let r = store.geo_add(
            &key,
            false,
            false,
            false,
            &[
                (13.361389, 38.115556, Bytes::from("Palermo")),
                (15.087269, 37.502669, Bytes::from("Catania")),
            ],
        );
        assert!(matches!(r, CommandResponse::Integer(2)));

        // GEOPOS
        let r = store.geo_pos(&key, &[Bytes::from("Palermo")]);
        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    CommandResponse::Array(coords) => {
                        assert_eq!(coords.len(), 2);
                    }
                    _ => panic!("expected array"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_geo_dist() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mygeo");

        store.geo_add(
            &key,
            false,
            false,
            false,
            &[
                (13.361389, 38.115556, Bytes::from("Palermo")),
                (15.087269, 37.502669, Bytes::from("Catania")),
            ],
        );

        let r = store.geo_dist(&key, &Bytes::from("Palermo"), &Bytes::from("Catania"), "km");
        match r {
            CommandResponse::BulkString(v) => {
                let dist: f64 = std::str::from_utf8(&v).unwrap().parse().unwrap();
                assert!((dist - 166.274).abs() < 1.0, "dist: {}", dist);
            }
            _ => panic!("expected bulk string, got {:?}", r),
        }
    }

    #[test]
    fn test_geo_search_radius() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mygeo");

        store.geo_add(
            &key,
            false,
            false,
            false,
            &[
                (13.361389, 38.115556, Bytes::from("Palermo")),
                (15.087269, 37.502669, Bytes::from("Catania")),
                (2.349014, 48.864716, Bytes::from("Paris")),
            ],
        );

        let r = store.geo_search(
            &key,
            &GeoFrom::LonLat(15.0, 37.0),
            &GeoBy::Radius(200.0, "km".to_string()),
            Some(true),
            None,
            false,
            false,
            false,
        );

        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2, "should find Palermo and Catania");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_geo_search_box() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mygeo");

        store.geo_add(
            &key,
            false,
            false,
            false,
            &[
                (13.361389, 38.115556, Bytes::from("Palermo")),
                (15.087269, 37.502669, Bytes::from("Catania")),
                (2.349014, 48.864716, Bytes::from("Paris")),
            ],
        );

        let r = store.geo_search(
            &key,
            &GeoFrom::LonLat(14.0, 38.0),
            &GeoBy::Box(400.0, 200.0, "km".to_string()),
            Some(true),
            None,
            false,
            false,
            false,
        );

        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 2, "should find Palermo and Catania");
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_geo_search_count_asc() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mygeo");

        store.geo_add(
            &key,
            false,
            false,
            false,
            &[
                (13.361389, 38.115556, Bytes::from("Palermo")),
                (15.087269, 37.502669, Bytes::from("Catania")),
                (2.349014, 48.864716, Bytes::from("Paris")),
            ],
        );

        // Search all with count=1, closest to Catania
        let r = store.geo_search(
            &key,
            &GeoFrom::LonLat(15.0, 37.5),
            &GeoBy::Radius(10000.0, "km".to_string()),
            Some(true),
            Some(1),
            false,
            false,
            false,
        );

        match r {
            CommandResponse::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    CommandResponse::BulkString(v) => assert_eq!(v, &Bytes::from("Catania")),
                    _ => panic!("expected bulk"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_geoadd_nx_xx_ch() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mygeo");

        // Add initial member
        store.geo_add(&key, false, false, false, &[(13.361389, 38.115556, Bytes::from("Palermo"))]);

        // NX: should not update existing
        let r = store.geo_add(&key, true, false, false, &[(15.0, 37.0, Bytes::from("Palermo"))]);
        assert!(matches!(r, CommandResponse::Integer(0)));

        // XX: should not add new members
        let r = store.geo_add(&key, false, true, false, &[(15.087269, 37.502669, Bytes::from("Catania"))]);
        assert!(matches!(r, CommandResponse::Integer(0)));

        // CH: report changed count
        let r = store.geo_add(&key, false, false, true, &[(14.0, 38.0, Bytes::from("Palermo"))]);
        assert!(matches!(r, CommandResponse::Integer(1)));
    }

    #[test]
    fn test_geo_wrongtype() {
        let mut store = ShardStore::new();
        let key = Bytes::from("mystring");
        store.set(key.clone(), RedisValue::String(Bytes::from("hello")), None);

        let r = store.geo_add(&key, false, false, false, &[(13.0, 38.0, Bytes::from("x"))]);
        assert!(matches!(r, CommandResponse::Error(_)));

        let r = store.geo_pos(&key, &[Bytes::from("x")]);
        assert!(matches!(r, CommandResponse::Error(_)));
    }

    #[test]
    fn test_geo_search_store() {
        let mut store = ShardStore::new();
        let src = Bytes::from("src");
        let dest = Bytes::from("dest");

        store.geo_add(
            &src,
            false,
            false,
            false,
            &[
                (13.361389, 38.115556, Bytes::from("Palermo")),
                (15.087269, 37.502669, Bytes::from("Catania")),
                (2.349014, 48.864716, Bytes::from("Paris")),
            ],
        );

        let r = store.geo_search_store(
            &dest,
            &src,
            &GeoFrom::LonLat(15.0, 37.0),
            &GeoBy::Radius(200.0, "km".to_string()),
            Some(true),
            None,
            false,
        );
        assert!(matches!(r, CommandResponse::Integer(2)));

        // dest should now be a zset with 2 members
        let entry = store.get(&dest).unwrap();
        match &entry.value {
            RedisValue::ZSet(zs) => assert_eq!(zs.len(), 2),
            _ => panic!("expected zset"),
        }
    }
}
