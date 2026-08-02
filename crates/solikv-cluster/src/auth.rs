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
//! <payload>|<ts_ms>|<nonce>|<hex hmac-sha256(secret, "ts:nonce:payload")>
//! ```
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
use std::collections::VecDeque;

type HmacSha256 = Hmac<Sha256>;

/// How far apart the clocks may be before a frame is refused.
///
/// Five minutes, matching `db/`. Wide enough for ordinary NTP drift, narrow
/// enough that a captured frame stops being useful quickly.
pub const MAX_SKEW_MS: u64 = 5 * 60 * 1_000;

/// Longest frame accepted, before parsing.
///
/// A gossip frame is a short line. The bound exists so a peer cannot make this
/// node buffer without limit before it has authenticated anything.
pub const MAX_FRAME_BYTES: usize = 8 * 1024;

/// How many recent nonces to remember.
///
/// The time window alone does not stop replay *inside* the window, which is
/// five minutes of free `UPDATE` messages. Bounded because it is fed by the
/// network: an unbounded set of seen nonces is a remote memory leak.
pub const NONCE_MEMORY: usize = 4_096;

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

impl Envelope {
    /// Wraps a payload for the wire. The returned line includes its terminator.
    pub fn seal(payload: &str, secret: &ClusterSecret, now_ms: u64, nonce: &str) -> String {
        let signature = secret.sign(&format!("{now_ms}:{nonce}:{payload}"));
        format!("{payload}|{now_ms}|{nonce}|{signature}\n")
    }

    /// Verifies a frame and returns its payload.
    ///
    /// The order of checks is deliberate: cheap and non-cryptographic first, so
    /// an unauthenticated peer cannot make this node do HMAC work by sending
    /// rubbish. The nonce is only remembered **after** the signature verifies,
    /// or an attacker could fill the replay memory with nonces it invented.
    pub fn open(
        frame: &str,
        secret: &ClusterSecret,
        now_ms: u64,
        seen: &mut NonceMemory,
    ) -> Result<String, AuthError> {
        let frame = frame.trim_end_matches(['\n', '\r']);
        if frame.len() > MAX_FRAME_BYTES {
            return Err(AuthError::TooLong(frame.len()));
        }

        // From the right: a payload may contain `|`, the three trailing fields
        // may not. Splitting from the left would let a crafted payload move the
        // field boundaries.
        let (rest, signature) = frame.rsplit_once('|').ok_or(AuthError::Malformed)?;
        let (rest, nonce) = rest.rsplit_once('|').ok_or(AuthError::Malformed)?;
        let (payload, timestamp) = rest.rsplit_once('|').ok_or(AuthError::Malformed)?;
        if payload.is_empty() || nonce.is_empty() || signature.is_empty() {
            return Err(AuthError::Malformed);
        }

        let ts: u64 = timestamp.parse().map_err(|_| AuthError::Malformed)?;
        let skew = ts.abs_diff(now_ms);
        if skew > MAX_SKEW_MS {
            return Err(AuthError::OutsideWindow { skew_ms: skew });
        }

        if seen.contains(nonce) {
            return Err(AuthError::Replay);
        }

        let expected = secret.sign(&format!("{ts}:{nonce}:{payload}"));
        if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
            return Err(AuthError::BadSignature);
        }

        seen.remember(nonce);
        Ok(payload.to_string())
    }
}

/// A bounded memory of recently accepted nonces.
#[derive(Debug, Default)]
pub struct NonceMemory {
    order: VecDeque<String>,
    set: std::collections::HashSet<String>,
}

impl NonceMemory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, nonce: &str) -> bool {
        self.set.contains(nonce)
    }

    pub fn remember(&mut self, nonce: &str) {
        if self.set.insert(nonce.to_string()) {
            self.order.push_back(nonce.to_string());
            while self.order.len() > NONCE_MEMORY {
                if let Some(oldest) = self.order.pop_front() {
                    self.set.remove(&oldest);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
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

    #[test]
    fn a_sealed_frame_opens() {
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal("PING n1 10.0.0.1 7000 42", &secret(), NOW, "abc123");
        assert!(frame.ends_with('\n'));
        assert_eq!(
            Envelope::open(&frame, &secret(), NOW, &mut seen).unwrap(),
            "PING n1 10.0.0.1 7000 42"
        );
    }

    #[test]
    fn an_unsigned_frame_is_refused() {
        // The whole point. The old server fed `GossipMessage::decode` straight
        // from the socket, so an `UPDATE` from anyone reachable rewrote the
        // topology.
        let mut seen = NonceMemory::new();
        let raw = "UPDATE attacker 10.0.0.9 7000 master\n";
        assert_eq!(
            Envelope::open(raw, &secret(), NOW, &mut seen),
            Err(AuthError::Malformed)
        );
    }

    #[test]
    fn a_frame_signed_with_another_secret_is_refused() {
        let mut seen = NonceMemory::new();
        let theirs = ClusterSecret::new("a-different-secret-also-32-bytes-long").unwrap();
        let frame = Envelope::seal("UPDATE n1 10.0.0.1 7000 master", &theirs, NOW, "n");
        assert_eq!(
            Envelope::open(&frame, &secret(), NOW, &mut seen),
            Err(AuthError::BadSignature)
        );
    }

    #[test]
    fn a_tampered_payload_is_refused() {
        // The signature covers the payload, so promoting yourself to master
        // after signing must fail rather than replicate.
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal("UPDATE n1 10.0.0.1 7000 slave", &secret(), NOW, "n");
        let altered = frame.replace("slave", "maste");
        assert_eq!(
            Envelope::open(&altered, &secret(), NOW, &mut seen),
            Err(AuthError::BadSignature)
        );
    }

    #[test]
    fn a_replayed_frame_is_refused_inside_the_time_window() {
        // The window alone leaves five minutes of free UPDATEs. This is what
        // makes the anti-replay actually anti-replay.
        let mut seen = NonceMemory::new();
        let frame = Envelope::seal("MEET n1 10.0.0.1 7000", &secret(), NOW, "once");
        assert!(Envelope::open(&frame, &secret(), NOW, &mut seen).is_ok());
        assert_eq!(
            Envelope::open(&frame, &secret(), NOW, &mut seen),
            Err(AuthError::Replay)
        );
    }

    #[test]
    fn a_stale_frame_is_refused_in_both_directions() {
        let mut seen = NonceMemory::new();
        let old = Envelope::seal(
            "PING n1 10.0.0.1 7000 1",
            &secret(),
            NOW - MAX_SKEW_MS - 1,
            "a",
        );
        assert!(matches!(
            Envelope::open(&old, &secret(), NOW, &mut seen),
            Err(AuthError::OutsideWindow { .. })
        ));
        // And a frame from the future, which is the same attack with the clock
        // the other way round.
        let ahead = Envelope::seal(
            "PING n1 10.0.0.1 7000 1",
            &secret(),
            NOW + MAX_SKEW_MS + 1,
            "b",
        );
        assert!(matches!(
            Envelope::open(&ahead, &secret(), NOW, &mut seen),
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
            "n",
        );
        let _ = Envelope::open(&forged, &secret(), NOW, &mut seen);
        assert!(
            seen.is_empty(),
            "a rejected frame polluted the nonce memory"
        );
    }

    #[test]
    fn the_nonce_memory_is_bounded() {
        // Fed by the network. Unbounded means a remote memory leak.
        let mut seen = NonceMemory::new();
        for i in 0..(NONCE_MEMORY + 500) {
            seen.remember(&format!("nonce-{i}"));
        }
        assert_eq!(seen.len(), NONCE_MEMORY);
        assert!(
            !seen.contains("nonce-0"),
            "the oldest should have been evicted"
        );
        assert!(seen.contains(&format!("nonce-{}", NONCE_MEMORY + 499)));
    }

    #[test]
    fn an_oversized_frame_is_refused_before_it_is_parsed() {
        let mut seen = NonceMemory::new();
        let huge = "A".repeat(MAX_FRAME_BYTES + 1);
        assert!(matches!(
            Envelope::open(&huge, &secret(), NOW, &mut seen),
            Err(AuthError::TooLong(_))
        ));
    }

    #[test]
    fn a_payload_containing_the_separator_still_verifies() {
        // Fields are split from the right precisely so a crafted payload cannot
        // move the boundaries. Splitting from the left would let
        // `UPDATE n|999|x|deadbeef` present forged trailing fields.
        let mut seen = NonceMemory::new();
        let payload = "UPDATE n1 10.0.0.1 7000 a|b|c";
        let frame = Envelope::seal(payload, &secret(), NOW, "n");
        assert_eq!(
            Envelope::open(&frame, &secret(), NOW, &mut seen).unwrap(),
            payload
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
        // A shared secret proves membership, not identity — every node can sign
        // as any node. This catches the confused and the replayed case, and it
        // is honestly all it catches.
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
    fn nonces_do_not_repeat() {
        let a = new_nonce();
        let b = new_nonce();
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn the_secret_is_not_printed_by_debug() {
        assert_eq!(format!("{:?}", secret()), "ClusterSecret(<redacted>)");
    }
}
