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
        request_timeout: Duration::from_secs(1),
        retry: RetryConfig::default(),
        breaker: BreakerConfig::default(),
        auth: AuthSettings::Public,
        tls: None,
        headers: Vec::new(),
    }
}
