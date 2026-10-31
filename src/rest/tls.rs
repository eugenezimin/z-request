//! TLS for `https://` endpoints: rustls with the `ring` provider, servers verified against the
//! platform's trust store plus `tls.ca_file`, and an optional client certificate for mTLS.
//!
//! No process-wide crypto provider is installed: each configuration carries its own, so the
//! host's own rustls setup is never affected.

use std::path::Path;
use std::sync::Arc;

use rustls::ClientConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_platform_verifier::Verifier;

use crate::config::TlsConfig;

/// The client configuration for a connection with `tls` settings (or none).
pub(crate) fn client_config(tls: Option<&TlsConfig>) -> Result<ClientConfig, String> {
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
        Verifier::new(Arc::clone(&provider))
    } else {
        Verifier::new_with_extra_roots(extra_roots, Arc::clone(&provider))
    }
    .map_err(|e| format!("cannot set up certificate verification: {e}"))?;
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("cannot set up TLS: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier));
    let identity = tls.and_then(|t| {
        t.client_cert_file
            .as_deref()
            .zip(t.client_key_file.as_deref())
    });
    let mut config = match identity {
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

fn certificates(path: &Path, field: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let fail = |e: &dyn std::fmt::Display| format!("{field} `{}`: {e}", path.display());
    CertificateDer::pem_file_iter(path)
        .map_err(|e| fail(&e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| fail(&e))
}
