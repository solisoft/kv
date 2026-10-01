//! TLS: one certificate, one CA, every channel.
//!
//! The server's certificate and key (`--tls-cert`, `--tls-key`) serve the
//! RESP and REST listeners, and are this node's client certificate on the
//! links it opens: to its master (`--tls-replication`), to `MIGRATE`
//! targets (same flag, as in Redis), and to the other nodes of the cluster
//! bus (`--tls-cluster`).
//!
//! The CA (`--tls-ca-cert`) does two jobs, as Redis' `tls-ca-cert-file`:
//!
//! - **Client certificates** on RESP and REST, per `--tls-auth-clients`:
//!   `yes` (the default once a CA is set) refuses a client without a
//!   certificate the CA signed, `optional` checks one only if presented,
//!   `no` never asks.
//! - **Peer certificates** on the links this node opens, and on inbound bus
//!   connections, which always require one: the bus is node-to-node only.
//!
//! Peer certificates are checked against the CA, not against the address
//! dialled — Redis does the same. Nodes are reached by IP, are renumbered
//! by failovers and announce addresses of their own choosing, so a name
//! check would demand an IP SAN per node and still prove less than the CA
//! does: anything holding a certificate from this private CA is a member.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig,
    SignatureScheme,
};

/// `--tls-auth-clients`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuth {
    No,
    Optional,
    Required,
}

impl ClientAuth {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "yes" | "true" | "1" => Ok(Self::Required),
            "optional" => Ok(Self::Optional),
            "no" | "false" | "0" => Ok(Self::No),
            other => Err(format!("expected yes, no or optional, got '{other}'")),
        }
    }
}

/// What the flags ask for.
#[derive(Debug, Clone)]
pub struct TlsOptions {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: Option<PathBuf>,
    /// `None`: `yes` when a CA is set (Redis' default), else `no`.
    pub auth_clients: Option<ClientAuth>,
    pub replication: bool,
    pub cluster: bool,
}

/// Loaded TLS material, ready for every listener and link.
#[derive(Clone)]
pub struct Tls {
    /// RESP and REST.
    pub server: Arc<ServerConfig>,
    /// Links this node opens (replication, `MIGRATE`, the bus); `None`
    /// without a CA, which outbound TLS requires.
    pub client: Option<Outbound>,
    /// Inbound cluster bus connections (a client certificate is required).
    pub bus_server: Option<Arc<ServerConfig>>,
    pub replication: bool,
    pub cluster: bool,
}

impl Tls {
    pub fn load(opts: &TlsOptions) -> Result<Self, String> {
        let certs = load_certs(&opts.cert)?;
        let key = load_key(&opts.key)?;
        let roots = match &opts.ca {
            Some(ca) => Some(Arc::new(load_roots(ca)?)),
            None => None,
        };
        let auth = opts.auth_clients.unwrap_or(if roots.is_some() {
            ClientAuth::Required
        } else {
            ClientAuth::No
        });
        let server = match (&roots, auth) {
            (_, ClientAuth::No) => ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs.clone(), key.clone_key()),
            (None, _) => {
                return Err("--tls-auth-clients needs --tls-ca-cert to check client \
                            certificates against"
                    .into())
            }
            (Some(roots), mode) => {
                let b = rustls::server::WebPkiClientVerifier::builder(roots.clone());
                let b = if mode == ClientAuth::Optional {
                    b.allow_unauthenticated()
                } else {
                    b
                };
                let verifier = b
                    .build()
                    .map_err(|e| format!("client certificate verifier: {e}"))?;
                ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(certs.clone(), key.clone_key())
            }
        }
        .map_err(|e| format!("TLS certificate/key: {e}"))?;

        let (client, bus_server) = match &roots {
            Some(roots) => {
                let verifier = Arc::new(CaOnlyVerifier::new(roots.clone())?);
                let client = ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(verifier)
                    .with_client_auth_cert(certs.clone(), key.clone_key())
                    .map_err(|e| format!("TLS client certificate: {e}"))?;
                let peer_verifier = rustls::server::WebPkiClientVerifier::builder(roots.clone())
                    .build()
                    .map_err(|e| format!("peer certificate verifier: {e}"))?;
                let bus = ServerConfig::builder()
                    .with_client_cert_verifier(peer_verifier)
                    .with_single_cert(certs, key)
                    .map_err(|e| format!("TLS certificate/key: {e}"))?;
                (Some(Outbound(Arc::new(client))), Some(Arc::new(bus)))
            }
            None => (None, None),
        };
        if (opts.replication || opts.cluster) && client.is_none() {
            return Err(
                "--tls-replication and --tls-cluster need --tls-ca-cert: the \
                        peer's certificate is checked against it"
                    .into(),
            );
        }
        Ok(Self {
            server: Arc::new(server),
            client,
            bus_server,
            replication: opts.replication,
            cluster: opts.cluster,
        })
    }

    /// The connector for replication links and `MIGRATE`, when they use TLS.
    pub fn replication_client(&self) -> Option<Outbound> {
        self.replication.then(|| self.client.clone()).flatten()
    }
}

/// A TLS client configuration for the links this node opens.
#[derive(Clone)]
pub struct Outbound(pub Arc<ClientConfig>);

impl std::fmt::Debug for Outbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Outbound(TLS)")
    }
}

impl Outbound {
    /// TLS over `tcp`, to `host` (only used for SNI; see the module doc).
    pub async fn connect(
        &self,
        host: &str,
        tcp: tokio::net::TcpStream,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        let name = server_name(host);
        tokio_rustls::TlsConnector::from(self.0.clone())
            .connect(name, tcp)
            .await
    }

    pub fn connector(&self) -> tokio_rustls::TlsConnector {
        tokio_rustls::TlsConnector::from(self.0.clone())
    }
}

/// A `ServerName` for `host`: its IP if it is one, else the name, else a
/// placeholder (the name is not what the peer is checked against).
pub fn server_name(host: &str) -> ServerName<'static> {
    let h = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = h.parse::<std::net::IpAddr>() {
        return ServerName::IpAddress(ip.into());
    }
    ServerName::try_from(h.to_string())
        .unwrap_or_else(|_| ServerName::try_from("solikv.invalid").expect("static name"))
}

/// Checks the peer's chain against the CA, and not its name (module doc).
#[derive(Debug)]
struct CaOnlyVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl CaOnlyVerifier {
    fn new(roots: Arc<RootCertStore>) -> Result<Self, String> {
        let inner = rustls::client::WebPkiServerVerifier::builder(roots)
            .build()
            .map_err(|e| format!("peer certificate verifier: {e}"))?;
        Ok(Self { inner })
    }
}

impl ServerCertVerifier for CaOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(rustls::Error::InvalidCertificate(e)) if is_name_mismatch(&e) => {
                Ok(ServerCertVerified::assertion())
            }
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The chain verified, only the name did not match. Webpki checks the name
/// after the chain, so this error means the CA did sign it.
fn is_name_mismatch(e: &CertificateError) -> bool {
    matches!(
        e,
        CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
    )
}

fn read_pem(path: &Path, what: &str) -> Result<Vec<pem::Pem>, String> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("cannot read {what} {}: {e}", path.display()))?;
    pem::parse_many(&bytes).map_err(|e| format!("cannot parse {what} {}: {e}", path.display()))
}

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs: Vec<CertificateDer<'static>> = read_pem(path, "TLS certificate")?
        .iter()
        .filter(|p| p.tag() == "CERTIFICATE")
        .map(|p| p.contents().to_vec().into())
        .collect();
    if certs.is_empty() {
        return Err(format!("no certificate in {}", path.display()));
    }
    Ok(certs)
}

/// PKCS#8, PKCS#1 (RSA) or SEC1 (EC) private keys.
pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    for p in read_pem(path, "TLS private key")? {
        let der = p.contents().to_vec();
        match p.tag() {
            "PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs8(der.into())),
            "RSA PRIVATE KEY" => return Ok(PrivateKeyDer::Pkcs1(der.into())),
            "EC PRIVATE KEY" => return Ok(PrivateKeyDer::Sec1(der.into())),
            _ => {}
        }
    }
    Err(format!("no private key in {}", path.display()))
}

fn load_roots(path: &Path) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| format!("CA certificate {}: {e}", path.display()))?;
    }
    Ok(roots)
}

/// A boxed stream: plain TCP or TLS, for code that serves both.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> AsyncStream for T {}

pub type BoxStream = Box<dyn AsyncStream>;

/// Connect to `host:port`, over TLS when `tls` is set.
pub async fn connect(host: &str, port: u16, tls: Option<&Outbound>) -> std::io::Result<BoxStream> {
    let tcp = tokio::net::TcpStream::connect((host, port)).await?;
    let _ = tcp.set_nodelay(true);
    Ok(match tls {
        Some(t) => Box::new(t.connect(host, tcp).await?),
        None => Box::new(tcp),
    })
}

/// One DER element at the start of `b`: its tag, its contents, and what
/// follows it.
fn der(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, b) = b.split_first()?;
    let (&first, mut b) = b.split_first()?;
    let len = if first < 0x80 {
        first as usize
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || b.len() < n {
            return None;
        }
        let len = b[..n].iter().fold(0usize, |l, &x| (l << 8) | x as usize);
        b = &b[n..];
        len
    };
    (b.len() >= len).then(|| (tag, &b[..len], &b[len..]))
}

/// The subject common name of a DER certificate (`--tls-auth-clients-user
/// CN`), if it has one in a string type. Only reads a certificate rustls has
/// already verified against the CA.
pub fn certificate_cn(cert: &[u8]) -> Option<String> {
    const SEQUENCE: u8 = 0x30;
    const SET: u8 = 0x31;
    const OID: u8 = 0x06;
    const COMMON_NAME: [u8; 3] = [0x55, 0x04, 0x03]; // 2.5.4.3
    let (SEQUENCE, cert, _) = der(cert)? else {
        return None;
    };
    let (SEQUENCE, tbs, _) = der(cert)? else {
        return None;
    };
    // [0] version (optional), then serial number, signature algorithm,
    // issuer and validity come before the subject.
    let mut rest = tbs;
    if let (0xA0, _, after) = der(rest)? {
        rest = after;
    }
    for _ in 0..4 {
        rest = der(rest)?.2;
    }
    let (SEQUENCE, mut rdns, _) = der(rest)? else {
        return None;
    };
    while !rdns.is_empty() {
        let (SET, mut set, next) = der(rdns)? else {
            return None;
        };
        rdns = next;
        while !set.is_empty() {
            let (SEQUENCE, attr, next) = der(set)? else {
                return None;
            };
            set = next;
            let (OID, oid, value) = der(attr)? else {
                return None;
            };
            if oid == COMMON_NAME {
                let (tag, v, _) = der(value)?;
                // UTF8String, PrintableString, T61String, IA5String.
                return match tag {
                    0x0C | 0x13 | 0x14 | 0x16 => std::str::from_utf8(v).ok().map(str::to_string),
                    _ => None,
                };
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert_with(dn: rcgen::DistinguishedName) -> Vec<u8> {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.distinguished_name = dn;
        let key = rcgen::KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().der().to_vec()
    }

    #[test]
    fn the_subject_common_name_is_read() {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::OrganizationName, "Acme");
        dn.push(rcgen::DnType::CommonName, "alice");
        assert_eq!(certificate_cn(&cert_with(dn)).as_deref(), Some("alice"));
    }

    #[test]
    fn no_common_name_and_garbage_read_as_none() {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::OrganizationName, "Acme");
        assert_eq!(certificate_cn(&cert_with(dn)), None);
        assert_eq!(certificate_cn(b""), None);
        assert_eq!(certificate_cn(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]), None);
        let mut cut = cert_with(rcgen::DistinguishedName::new());
        cut.truncate(cut.len() / 2);
        assert_eq!(certificate_cn(&cut), None);
    }
}
