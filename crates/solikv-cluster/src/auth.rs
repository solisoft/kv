//! Authenticating the cluster bus.
//!
//! Gossip decides who is in the cluster and which node owns which slots. An
//! unauthenticated bus is therefore not "a bus without encryption" — it is a
//! stranger deciding the topology, and `UPDATE` is enough on its own: claim a
//! master id, claim a slot range, and reads for those keys go to you.
//!
//! The construction is the same one `db/src/cluster/transport.rs` uses, on
//! purpose. Two hand-rolled envelopes in one stack are two sets of replay bugs.
//!
//! ```text
//! <payload>|<ts_ms>|<nonce>|<receiver>|<hex hmac-sha256(secret, material)>
//! material = "v2\n" + for each of ts, nonce, receiver, payload: "<len>:<field>\n"
//! ```
//!
//! Every field of the MAC input is length-prefixed, so no byte of one field
//! can be read as part of another: the earlier `ts:nonce:payload` let a `:` in
//! the payload slide into the nonce.
//!
//! `receiver` is the node id the frame is addressed to. A frame is only
//! accepted by that node, so a capture cannot be replayed at the rest of the
//! cluster. `*` means "whoever answers at this address" and exists for the
//! handshake — before a peer's pong, its node id is not known. Only frames
//! that carry no authority (`PING`, `MEET`) may be unbound; the caller decides
//! that from [`Opened::receiver_bound`].
//!
//! # There is no optional mode
//!
//! [`Envelope::seal`] and [`Envelope::open`] take a `&ClusterSecret`, and
//! [`ClusterSecret::new`] rejects anything short. There is no `Option`, so
//! there is no branch that falls through to sending in the clear — which is
//! exactly the shape of the bug that had to be fixed in `db/` and guarded
//! against in `es/`. The same mistake three times in one stack would be a
//! pattern, not an accident.
//!
//! # What a shared secret does and does not prove
//!
//! It proves the sender **is a cluster member**. It does not prove *which*
//! member: every node holds the same key, so any node can sign a frame naming
//! any node_id. Per-node identity needs per-node keys, and this does not have
//! them.
//!
//! That limit is why [`SenderIdentity`] exists. It pins the identity a
//! connection claimed in its first frame and rejects later frames on that
//! connection that claim a different one. That stops a replayed or confused
//! frame, and it does **not** stop a malicious member impersonating a peer.
//! Saying otherwise would be the comfortable claim and the false one.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// How far apart the clocks may be before a frame is refused.
///
/// Five minutes, matching `db/`. Wide enough for ordinary NTP drift, narrow
/// enough that a captured frame stops being useful quickly.
pub const MAX_SKEW_MS: u64 = 5 * 60 * 1_000;

/// The receiver of a frame sent before the peer's node id is known.
pub const UNBOUND_RECEIVER: &str = "*";

/// Longest receiver field. Node ids are short; this only bounds parsing.
const MAX_RECEIVER_LEN: usize = 128;

/// Length of a nonce on the wire: 16 random bytes, hex.
pub const NONCE_HEX_LEN: usize = 32;

/// Longest frame accepted, before parsing.
///
/// A frame carries a node's header (its slots, as ranges) and its view of the
/// other nodes, or a `PUBLISH` relayed cluster-wide; 1 MiB holds the worst
/// fragmented slot map with room to spare. The bound exists so a peer cannot
/// make this node buffer without limit before it has authenticated anything
/// (the bus also caps its connections, so the total is bounded too).
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// How many nonces may be remembered at once.
///
/// The time window alone does not stop replay *inside* the window, which is
/// ten minutes (±5) of free `UPDATE` messages. Every nonce accepted is
/// therefore kept until its timestamp leaves the window — not merely the last
/// few thousand, which a busy cluster cycles through in seconds, re-opening
/// replay. Bounded because it is fed by the network; only verified frames can
/// fill it, and a full memory refuses new frames rather than forgetting live
/// ones. 262 144 covers ~430 frames a second sustained for the whole window.
pub const NONCE_MEMORY: usize = 262_144;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("cluster secret is too short: {0} bytes, need at least 32")]
    WeakSecret(usize),
    #[error("frame is {0} bytes, over the {MAX_FRAME_BYTES} limit")]
    TooLong(usize),
    #[error("frame is not a signed cluster frame")]
    Malformed,
    #[error("frame timestamp is {skew_ms}ms away from now, outside the {MAX_SKEW_MS}ms window")]
    OutsideWindow { skew_ms: u64 },
    #[error("frame nonce has already been seen — replay")]
    Replay,
    #[error("frame nonce is not {NONCE_HEX_LEN} hex characters")]
    BadNonce,
    #[error("frame is addressed to {0:?}, not this node")]
    WrongReceiver(String),
    #[error("nonce memory is full; refusing frames until older nonces expire")]
    NonceMemoryFull,
    #[error("frame signature does not verify")]
    BadSignature,
    #[error("frame claims node {claimed:?} on a connection that identified as {established:?}")]
    IdentitySwitch {
        claimed: String,
        established: String,
    },
}

/// The shared cluster secret.
///
/// A newtype so it cannot be confused with any other string, and so the
/// minimum length is checked once at construction rather than at each use.
#[derive(Clone)]
pub struct ClusterSecret(Vec<u8>);

impl std::fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterSecret(<redacted>)")
    }
}

impl ClusterSecret {
    /// Rejects a short secret rather than accepting it.
    ///
    /// 32 bytes. This key is the entire authentication of the topology, and a
    /// guessable one is the same as none — except that it looks configured.
    pub fn new(secret: impl AsRef<[u8]>) -> Result<Self, AuthError> {
        let bytes = secret.as_ref();
        if bytes.len() < 32 {
            return Err(AuthError::WeakSecret(bytes.len()));
        }
        Ok(Self(bytes.to_vec()))
    }

    fn sign(&self, material: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.0).expect("HMAC accepts any key length");
        mac.update(material.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}

/// One signed frame.
pub struct Envelope;

/// What [`Envelope::open`] hands back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opened {
    pub payload: String,
    /// The frame named this node as its receiver, rather than
    /// [`UNBOUND_RECEIVER`]. Anything that changes ownership or liveness must
    /// require it, or a capture can be replayed at every other node.
    pub receiver_bound: bool,
}

fn mac_material(ts: u64, nonce: &str, receiver: &str, payload: &str) -> String {
    let ts = ts.to_string();
    format!(
        "v2\n{}:{}\n{}:{}\n{}:{}\n{}:{}\n",
        ts.len(),
        ts,
        nonce.len(),
        nonce,
        receiver.len(),
        receiver,
        payload.len(),
        payload
    )
}

impl Envelope {
    /// Wraps a payload for the wire. The returned line includes its terminator.
    ///
    /// `receiver` is the node id of the peer, or [`UNBOUND_RECEIVER`] when it
    /// is not known yet.
    pub fn seal(
        payload: &str,
        secret: &ClusterSecret,
        now_ms: u64,
        nonce: &str,
        receiver: &str,
    ) -> String {
        let signature = secret.sign(&mac_material(now_ms, nonce, receiver, payload));
        format!("{payload}|{now_ms}|{nonce}|{receiver}|{signature}\n")
    }

    /// Verifies a frame addressed to `me` and returns its payload.
    ///
    /// The order of checks is deliberate: cheap and non-cryptographic first, so
    /// an unauthenticated peer cannot make this node do HMAC work by sending
    /// rubbish. The nonce is only remembered **after** the signature verifies,
    /// or an attacker could fill the replay memory with nonces it invented —
    /// and remembering is what detects the replay, so two copies racing on two
    /// connections cannot both get through.
    pub fn open(
        frame: &str,
        secret: &ClusterSecret,
        now_ms: u64,
        seen: &mut NonceMemory,
        me: &str,
    ) -> Result<Opened, AuthError> {
        let frame = frame.trim_end_matches(['\n', '\r']);
        if frame.len() > MAX_FRAME_BYTES {
            return Err(AuthError::TooLong(frame.len()));
        }

        // From the right: a payload may contain `|`, the four trailing fields
        // may not. Splitting from the left would let a crafted payload move the
        // field boundaries.
        let (rest, signature) = frame.rsplit_once('|').ok_or(AuthError::Malformed)?;
        let (rest, receiver) = rest.rsplit_once('|').ok_or(AuthError::Malformed)?;
        let (rest, nonce) = rest.rsplit_once('|').ok_or(AuthError::Malformed)?;
        let (payload, timestamp) = rest.rsplit_once('|').ok_or(AuthError::Malformed)?;
        if payload.is_empty() || signature.is_empty() {
            return Err(AuthError::Malformed);
        }
        if receiver.is_empty() || receiver.len() > MAX_RECEIVER_LEN {
            return Err(AuthError::Malformed);
        }
        let nonce_key = parse_nonce(nonce)?;

        let ts: u64 = timestamp.parse().map_err(|_| AuthError::Malformed)?;
        let skew = ts.abs_diff(now_ms);
        if skew > MAX_SKEW_MS {
            return Err(AuthError::OutsideWindow { skew_ms: skew });
        }

        let receiver_bound = receiver == me;
        if !receiver_bound && receiver != UNBOUND_RECEIVER {
            return Err(AuthError::WrongReceiver(receiver.to_string()));
        }

        if seen.contains_key(&nonce_key) {
            return Err(AuthError::Replay);
        }

        let expected = secret.sign(&mac_material(ts, nonce, receiver, payload));
        if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
            return Err(AuthError::BadSignature);
        }

        seen.remember_key(nonce_key, ts, now_ms)?;
        Ok(Opened {
            payload: payload.to_string(),
            receiver_bound,
        })
    }
}

fn parse_nonce(nonce: &str) -> Result<[u8; 16], AuthError> {
    if nonce.len() != NONCE_HEX_LEN {
        return Err(AuthError::BadNonce);
    }
    let mut key = [0u8; 16];
    hex::decode_to_slice(nonce, &mut key).map_err(|_| AuthError::BadNonce)?;
    Ok(key)
}

/// Every nonce accepted and still inside the time window.
///
/// Shared by every bus connection. It used to be per connection, and the
/// sender opens a connection per frame, so each memory saw one frame and
/// replay detection never fired.
#[derive(Debug, Default)]
pub struct NonceMemory {
    /// nonce → the instant after which its frame is outside the window anyway.
    expiries: std::collections::HashMap<[u8; 16], u64>,
    inserts_since_sweep: usize,
}

impl NonceMemory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, nonce: &str) -> bool {
        parse_nonce(nonce).is_ok_and(|key| self.contains_key(&key))
    }

    fn contains_key(&self, key: &[u8; 16]) -> bool {
        self.expiries.contains_key(key)
    }

    /// Remembers a nonce whose frame carried timestamp `ts_ms`.
    pub fn remember(&mut self, nonce: &str, ts_ms: u64, now_ms: u64) -> Result<(), AuthError> {
        self.remember_key(parse_nonce(nonce)?, ts_ms, now_ms)
    }

    fn remember_key(&mut self, key: [u8; 16], ts_ms: u64, now_ms: u64) -> Result<(), AuthError> {
        if self.expiries.contains_key(&key) {
            return Err(AuthError::Replay);
        }
        self.inserts_since_sweep += 1;
        if self.inserts_since_sweep >= 1024 || self.expiries.len() >= NONCE_MEMORY {
            self.sweep(now_ms);
        }
        if self.expiries.len() >= NONCE_MEMORY {
            return Err(AuthError::NonceMemoryFull);
        }
        self.expiries.insert(key, ts_ms.saturating_add(MAX_SKEW_MS));
        Ok(())
    }

    /// Forgets nonces whose frames could no longer pass the time check.
    pub fn sweep(&mut self, now_ms: u64) {
        self.expiries.retain(|_, expiry| *expiry >= now_ms);
        self.inserts_since_sweep = 0;
    }

    pub fn len(&self) -> usize {
        self.expiries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.expiries.is_empty()
    }
}

/// The node identity a single connection has claimed.
///
/// Pins whatever the first frame said and refuses a later frame on the same
/// connection that says something else. See the module note on what a shared
/// secret can and cannot prove: this catches confusion and replay across
/// connections, not a malicious member.
#[derive(Debug, Default)]
pub struct SenderIdentity(Option<String>);

impl SenderIdentity {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn established(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Records or checks the identity a frame claims.
    pub fn observe(&mut self, claimed: &str) -> Result<(), AuthError> {
        match &self.0 {
            None => {
                self.0 = Some(claimed.to_string());
                Ok(())
            }
            Some(established) if established == claimed => Ok(()),
            Some(established) => Err(AuthError::IdentitySwitch {
                claimed: claimed.to_string(),
                established: established.clone(),
            }),
        }
    }
}

/// A fresh nonce.
pub fn new_nonce() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret() -> ClusterSecret {
        ClusterSecret::new("a-cluster-secret-of-at-least-32-bytes").unwrap()
    }

    const NOW: u64 = 1_800_000_000_000;
    const ME: &str = "node-me";

    fn nonce(n: u8) -> String {
        format!("{:032x}", n)
    }

    fn open(frame: &str, seen: &mut NonceMemory) -> Result<Opened, AuthError> {
        Envelope::open(frame, &secret(), NOW, seen, ME)
    }

    #[test]
    fn a_sealed_frame_opens() {
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal("PING n1 10.0.0.1 7000 42", &secret(), NOW, &nonce(1), ME);
        assert!(frame.ends_with('\n'));
        let opened = open(&frame, &mut seen).unwrap();
        assert_eq!(opened.payload, "PING n1 10.0.0.1 7000 42");
        assert!(opened.receiver_bound);
    }

    #[test]
    fn an_unbound_frame_opens_but_says_so() {
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal("PING n1 10.0.0.1 7000 42", &secret(), NOW, &nonce(1), "*");
        let opened = open(&frame, &mut seen).unwrap();
        assert!(!opened.receiver_bound);
    }

    #[test]
    fn a_frame_addressed_to_another_node_is_refused() {
        // A capture of A→B replayed at C. The MAC covers the receiver, so the
        // attacker cannot re-address it either.
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal(
            "UPDATE n1 10.0.0.1 7000 master - 0-100",
            &secret(),
            NOW,
            &nonce(1),
            "node-b",
        );
        assert_eq!(
            open(&frame, &mut seen),
            Err(AuthError::WrongReceiver("node-b".into()))
        );
        let readdressed = frame.replace("|node-b|", &format!("|{ME}|"));
        assert_eq!(open(&readdressed, &mut seen), Err(AuthError::BadSignature));
    }

    #[test]
    fn an_unsigned_frame_is_refused() {
        // The whole point. The old server fed `GossipMessage::decode` straight
        // from the socket, so an `UPDATE` from anyone reachable rewrote the
        // topology.
        let mut seen = NonceMemory::new();
        let raw = "UPDATE attacker 10.0.0.9 7000 master\n";
        assert_eq!(open(raw, &mut seen), Err(AuthError::Malformed));
    }

    #[test]
    fn a_frame_signed_with_another_secret_is_refused() {
        let mut seen = NonceMemory::new();
        let theirs = ClusterSecret::new("a-different-secret-also-32-bytes-long").unwrap();
        let frame = Envelope::seal(
            "UPDATE n1 10.0.0.1 7000 master",
            &theirs,
            NOW,
            &nonce(1),
            ME,
        );
        assert_eq!(open(&frame, &mut seen), Err(AuthError::BadSignature));
    }

    #[test]
    fn a_tampered_payload_is_refused() {
        // The signature covers the payload, so promoting yourself to master
        // after signing must fail rather than replicate.
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal(
            "UPDATE n1 10.0.0.1 7000 slave",
            &secret(),
            NOW,
            &nonce(1),
            ME,
        );
        let altered = frame.replace("slave", "maste");
        assert_eq!(open(&altered, &mut seen), Err(AuthError::BadSignature));
    }

    #[test]
    fn a_replayed_frame_is_refused_inside_the_time_window() {
        // The window alone leaves ten minutes of free UPDATEs. This is what
        // makes the anti-replay actually anti-replay.
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal("MEET n1 10.0.0.1 7000", &secret(), NOW, &nonce(7), ME);
        assert!(open(&frame, &mut seen).is_ok());
        assert_eq!(open(&frame, &mut seen), Err(AuthError::Replay));
    }

    #[test]
    fn a_replay_is_caught_however_many_frames_came_in_between() {
        // The old memory kept the last 4096 nonces, which a busy cluster cycles
        // through in seconds. What matters is the time window, not a count.
        let mut seen = NonceMemory::new();
        let first = Envelope::seal("PING a 10.0.0.1 7000 1", &secret(), NOW, &nonce(0), ME);
        assert!(open(&first, &mut seen).is_ok());
        for i in 1..10_000u32 {
            seen.remember(&format!("{:032x}", i + 1_000_000), NOW, NOW)
                .unwrap();
        }
        assert_eq!(open(&first, &mut seen), Err(AuthError::Replay));
    }

    #[test]
    fn a_stale_frame_is_refused_in_both_directions() {
        let mut seen = NonceMemory::new();
        let old = Envelope::seal(
            "PING n1 10.0.0.1 7000 1",
            &secret(),
            NOW - MAX_SKEW_MS - 1,
            &nonce(1),
            ME,
        );
        assert!(matches!(
            open(&old, &mut seen),
            Err(AuthError::OutsideWindow { .. })
        ));
        // And a frame from the future, which is the same attack with the clock
        // the other way round.
        let ahead = Envelope::seal(
            "PING n1 10.0.0.1 7000 1",
            &secret(),
            NOW + MAX_SKEW_MS + 1,
            &nonce(2),
            ME,
        );
        assert!(matches!(
            open(&ahead, &mut seen),
            Err(AuthError::OutsideWindow { .. })
        ));
    }

    #[test]
    fn the_nonce_is_only_remembered_after_the_signature_verifies() {
        // Otherwise an unauthenticated peer fills the replay memory with
        // nonces it invented, evicting the real ones and re-enabling replay.
        let mut seen = NonceMemory::new();
        let forged = Envelope::seal(
            "PING n1 10.0.0.1 7000 1",
            &ClusterSecret::new("x".repeat(32)).unwrap(),
            NOW,
            &nonce(1),
            ME,
        );
        let _ = open(&forged, &mut seen);
        assert!(
            seen.is_empty(),
            "a rejected frame polluted the nonce memory"
        );
    }

    #[test]
    fn expired_nonces_are_forgotten_and_the_memory_is_bounded() {
        let mut seen = NonceMemory::new();
        seen.remember(&nonce(1), NOW, NOW).unwrap();
        // Once the frame's timestamp is outside the window, its nonce can go:
        // the window check refuses a replay of it anyway.
        seen.sweep(NOW + MAX_SKEW_MS + 1);
        assert!(seen.is_empty());

        // Full of live nonces: new frames are refused rather than old nonces
        // forgotten, which would re-open replay for them.
        for i in 0..NONCE_MEMORY {
            seen.remember(&format!("{:032x}", i + 10), NOW, NOW)
                .unwrap();
        }
        assert_eq!(seen.len(), NONCE_MEMORY);
        assert_eq!(
            seen.remember(&nonce(2), NOW, NOW),
            Err(AuthError::NonceMemoryFull)
        );
        // …and recovers once they expire.
        assert!(seen.remember(&nonce(2), NOW, NOW + MAX_SKEW_MS + 1).is_ok());
        assert_eq!(seen.len(), 1);
    }

    #[test]
    fn a_malformed_nonce_is_refused() {
        let mut seen = NonceMemory::new();
        for bad in ["", "abc", &"g".repeat(32), &"a".repeat(33)] {
            let frame = Envelope::seal("PING n1 10.0.0.1 7000 1", &secret(), NOW, bad, ME);
            let res = open(&frame, &mut seen);
            assert!(
                matches!(res, Err(AuthError::BadNonce) | Err(AuthError::Malformed)),
                "{bad:?} gave {res:?}"
            );
        }
    }

    #[test]
    fn an_oversized_frame_is_refused_before_it_is_parsed() {
        let mut seen = NonceMemory::new();
        let huge = "A".repeat(MAX_FRAME_BYTES + 1);
        assert!(matches!(open(&huge, &mut seen), Err(AuthError::TooLong(_))));
    }

    #[test]
    fn a_payload_containing_the_separator_still_verifies() {
        // Fields are split from the right precisely so a crafted payload cannot
        // move the boundaries.
        let mut seen = NonceMemory::new();
        let payload = "UPDATE n1 10.0.0.1 7000 a|b|c";
        let frame = Envelope::seal(payload, &secret(), NOW, &nonce(1), ME);
        assert_eq!(open(&frame, &mut seen).unwrap().payload, payload);
    }

    #[test]
    fn the_mac_input_is_unambiguous() {
        // Length prefixes: moving bytes between fields changes the material.
        assert_ne!(
            mac_material(1, "ab", "*", "c:d"),
            mac_material(1, "ab:c", "*", "d")
        );
    }

    #[test]
    fn a_short_secret_is_refused_rather_than_used() {
        // A guessable key is the same as none, except that it looks configured.
        assert!(matches!(
            ClusterSecret::new("short"),
            Err(AuthError::WeakSecret(5))
        ));
        assert!(ClusterSecret::new("y".repeat(32)).is_ok());
    }

    #[test]
    fn a_connection_cannot_change_which_node_it_claims_to_be() {
        let mut identity = SenderIdentity::new();
        assert!(identity.observe("node-a").is_ok());
        assert!(identity.observe("node-a").is_ok());
        assert_eq!(
            identity.observe("node-b"),
            Err(AuthError::IdentitySwitch {
                claimed: "node-b".into(),
                established: "node-a".into()
            })
        );
        assert_eq!(identity.established(), Some("node-a"));
    }

    #[test]
    fn nonces_do_not_repeat_and_have_the_wire_shape() {
        let a = new_nonce();
        let b = new_nonce();
        assert_ne!(a, b);
        assert_eq!(a.len(), NONCE_HEX_LEN);
        assert!(parse_nonce(&a).is_ok());
    }

    #[test]
    fn the_secret_is_not_printed_by_debug() {
        assert_eq!(format!("{:?}", secret()), "ClusterSecret(<redacted>)");
    }
}
