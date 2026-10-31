//! TLS for `https://` endpoints of both transports: servers verified against the platform's
//! trust store plus `tls.ca_file`, and an optional client certificate for mTLS.
//!
//! The REST transport gets a whole rustls configuration with the `ring` provider; the gRPC
//! transport hands the same verifier and the client certificate to tonic. No process-wide
//! crypto provider is installed, so the host's own rustls setup is never affected.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls_platform_verifier::Verifier;

use crate::config::TlsConfig;

/// Verifies servers against the platform's trust store plus `tls.ca_file`.
pub(crate) fn verifier(tls: Option<&TlsConfig>) -> Result<Arc<Verifier>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let extra_roots = match tls.and_then(|t| t.ca_file.as_deref()) {
        Some(path) => {
            let certs = certificates(path, "ca_file")?;
            if certs.is_empty() {
                return Err(format!("ca_file `{}` holds no certificate", path.display()));
            }
            certs
        }
        None => Vec::new(),
    };
    let verifier = if extra_roots.is_empty() {
        Verifier::new(provider)
    } else {
        Verifier::new_with_extra_roots(extra_roots, provider)
    };
    verifier
        .map(Arc::new)
        .map_err(|e| format!("cannot set up certificate verification: {e}"))
}

/// The complete client configuration for a REST connection with `tls` settings (or none).
#[cfg(feature = "rest")]
pub(crate) fn client_config(tls: Option<&TlsConfig>) -> Result<rustls::ClientConfig, String> {
    use rustls::pki_types::PrivateKeyDer;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("cannot set up TLS: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(verifier(tls)?);
    let mut config = match identity_files(tls) {
        Some((cert, key)) => {
            let chain = certificates(cert, "client_cert_file")?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|e| format!("client_key_file `{}`: {e}", key.display()))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| format!("client certificate: {e}"))?
        }
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// A client certificate chain and its key, as PEM.
#[cfg(feature = "grpc")]
pub(crate) struct IdentityPem {
    pub(crate) cert: Vec<u8>,
    pub(crate) key: Vec<u8>,
}

/// The client certificate and key as PEM, for a gRPC connection with mTLS.
#[cfg(feature = "grpc")]
pub(crate) fn identity_pem(tls: Option<&TlsConfig>) -> Result<Option<IdentityPem>, String> {
    let Some((cert, key)) = identity_files(tls) else {
        return Ok(None);
    };
    let read = |path: &Path, field: &str| {
        std::fs::read(path).map_err(|e| format!("{field} `{}`: {e}", path.display()))
    };
    // Parse now, so a bad file is a configuration error rather than a failed connect.
    certificates(cert, "client_cert_file")?;
    rustls::pki_types::PrivateKeyDer::from_pem_file(key)
        .map_err(|e| format!("client_key_file `{}`: {e}", key.display()))?;
    Ok(Some(IdentityPem {
        cert: read(cert, "client_cert_file")?,
        key: read(key, "client_key_file")?,
    }))
}

fn identity_files(tls: Option<&TlsConfig>) -> Option<(&Path, &Path)> {
    tls.and_then(|t| {
        t.client_cert_file
            .as_deref()
            .zip(t.client_key_file.as_deref())
    })
}

fn certificates(path: &Path, field: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let fail = |e: &dyn std::fmt::Display| format!("{field} `{}`: {e}", path.display());
    CertificateDer::pem_file_iter(path)
        .map_err(|e| fail(&e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fail(&e))
}
