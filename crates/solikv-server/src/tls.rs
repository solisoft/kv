use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::sync::Arc;

/// Build a rustls `ServerConfig` from PEM files. Optional client CA enables mTLS.
pub fn build_server_config(
    cert_path: &str,
    key_path: &str,
    client_ca_path: Option<&str>,
) -> Result<ServerConfig, String> {
    let cert_pem =
        std::fs::read(cert_path).map_err(|e| format!("Failed to read TLS certificate: {e}"))?;
    let key_pem =
        std::fs::read(key_path).map_err(|e| format!("Failed to read TLS private key: {e}"))?;

    let certs_pem =
        pem::parse_many(&cert_pem).map_err(|e| format!("Failed to parse TLS certificate: {e}"))?;
    let keys_pem =
        pem::parse_many(&key_pem).map_err(|e| format!("Failed to parse TLS private key: {e}"))?;

    let certs: Vec<CertificateDer<'static>> = certs_pem
        .iter()
        .filter(|p| p.tag() == "CERTIFICATE")
        .map(|p| p.contents().to_vec().into())
        .collect();
    if certs.is_empty() {
        return Err("No CERTIFICATE blocks found in TLS cert file".into());
    }

    // Take the first block that actually parses as DER rather than the first block
    // whose tag looks right: a key file may lead with an unusable block (an OpenSSH
    // key, say) and still carry a valid PKCS#8 key further down.
    let key = keys_pem
        .iter()
        .filter(|p| {
            matches!(
                p.tag(),
                "PRIVATE KEY" | "RSA PRIVATE KEY" | "EC PRIVATE KEY"
            )
        })
        .find_map(|p| PrivateKeyDer::try_from(p.contents().to_vec()).ok())
        .ok_or_else(|| {
            "No usable private key in TLS key file (expected a PKCS#8, RSA, or SEC1 PEM \
             block; OpenSSH-format keys are not supported)"
                .to_string()
        })?;

    let builder = if let Some(ca_path) = client_ca_path {
        let ca_pem =
            std::fs::read(ca_path).map_err(|e| format!("Failed to read TLS client CA: {e}"))?;
        let ca_blocks =
            pem::parse_many(&ca_pem).map_err(|e| format!("Failed to parse TLS client CA: {e}"))?;
        let mut roots = RootCertStore::empty();
        for p in ca_blocks.iter().filter(|p| p.tag() == "CERTIFICATE") {
            let der = CertificateDer::from(p.contents().to_vec());
            roots
                .add(der)
                .map_err(|e| format!("Failed to add client CA: {e}"))?;
        }
        if roots.is_empty() {
            return Err("No CERTIFICATE blocks found in TLS client CA file".into());
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("Failed to build client cert verifier: {e}"))?;
        ServerConfig::builder().with_client_cert_verifier(verifier)
    } else {
        ServerConfig::builder().with_no_client_auth()
    };

    builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("Failed to create TLS config: {e}"))
}
