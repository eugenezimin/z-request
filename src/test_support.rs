//! Helpers shared by unit tests.

use std::time::Duration;

use crate::config::{
    AuthSettings, BreakerConfig, ConnectionSettings, Endpoint, Limit, RetryConfig,
};
use crate::utils::spi::Protocol;

/// A REST connection `name` at `http://<name>.test` with default settings: max 1, public auth,
/// 1 s connect and request timeouts.
pub(crate) fn connection(name: &str) -> ConnectionSettings {
    ConnectionSettings {
        name: name.into(),
        protocol: Protocol::Rest,
        endpoint: Endpoint {
            url: format!("http://{name}.test"),
            secure: false,
            host: format!("{name}.test"),
            port: 80,
            base_path: String::new(),
        },
        min: 0,
        max: Limit::Finite(1),
        connect_timeout: Duration::from_secs(1),
        max_response_size: crate::config::DEFAULT_MAX_RESPONSE_SIZE,
        http2: crate::config::Http2Mode::Auto,
        streams_per_connection: crate::config::DEFAULT_STREAMS_PER_CONNECTION,
        max_connection_age: None,
        request_timeout: Duration::from_secs(1),
        retry: RetryConfig::default(),
        breaker: BreakerConfig::default(),
        auth: AuthSettings::Public,
        tls: None,
        headers: Vec::new(),
    }
}

#[cfg(all(feature = "tls", any(feature = "rest", feature = "grpc")))]
pub(crate) use pki::Pki;

#[cfg(all(feature = "tls", any(feature = "rest", feature = "grpc")))]
mod pki {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::CertificateDer;

    /// A test CA, a server certificate for `localhost` and `svc.internal`, and a client
    /// certificate, written as PEM files to a fresh directory.
    // Each transport's tests read only the forms they need (DER for rustls, PEM for tonic).
    #[cfg_attr(not(all(feature = "rest", feature = "grpc")), allow(dead_code))]
    pub(crate) struct Pki {
        dir: PathBuf,
        pub(crate) ca: CertificateDer<'static>,
        pub(crate) ca_pem: String,
        pub(crate) server_chain: Vec<CertificateDer<'static>>,
        pub(crate) server_pem: String,
        pub(crate) server_key: String,
    }

    impl Pki {
        pub(crate) fn new() -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "z-request-tls-{}-{}",
                std::process::id(),
                N.fetch_add(1, SeqCst)
            ));
            std::fs::create_dir_all(&dir).unwrap();

            // Validity within the current and the next year (below the 825 days macOS allows).
            let year = 1970
                + (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    / 31_556_952) as i32;
            let validity = |p: &mut CertificateParams| {
                p.not_before = rcgen::date_time_ymd(year, 1, 1);
                p.not_after = rcgen::date_time_ymd(year + 1, 12, 31);
            };

            let ca_key = KeyPair::generate().unwrap();
            let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
            ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca.distinguished_name
                .push(DnType::CommonName, "z-request test CA");
            ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            validity(&mut ca);
            let ca_cert = ca.self_signed(&ca_key).unwrap();
            let issuer = Issuer::new(ca, ca_key);

            let leaf = |names: Vec<String>, purpose: ExtendedKeyUsagePurpose| {
                let key = KeyPair::generate().unwrap();
                let mut p = CertificateParams::new(names).unwrap();
                p.extended_key_usages = vec![purpose];
                validity(&mut p);
                (p.signed_by(&key, &issuer).unwrap(), key)
            };
            let (server, server_key) = leaf(
                vec!["localhost".into(), "svc.internal".into()],
                ExtendedKeyUsagePurpose::ServerAuth,
            );
            let (client, client_key) =
                leaf(vec!["client".into()], ExtendedKeyUsagePurpose::ClientAuth);

            let write = |name: &str, pem: String| std::fs::write(dir.join(name), pem).unwrap();
            write("ca.pem", ca_cert.pem());
            write("client.pem", client.pem());
            write("client.key", client_key.serialize_pem());
            Self {
                dir,
                ca: ca_cert.der().clone(),
                ca_pem: ca_cert.pem(),
                server_chain: vec![server.der().clone()],
                server_pem: server.pem(),
                server_key: server_key.serialize_pem(),
            }
        }

        pub(crate) fn file(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }
    }

    impl Drop for Pki {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}
