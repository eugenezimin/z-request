//! [`RestClient`]: requests to one REST connection, with credentials, trace headers, the pool
//! and the policies.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::connector::{RestConnection, RestConnector};
use crate::config::ConnectionSettings;
use crate::credentials::{AUTHORIZATION, RESERVED_HEADERS, outbound_headers};
use crate::policy::{ExponentialBackoff, Policies};
use crate::pool::TypedPool;
use crate::utils::error::error_text;
use crate::utils::error::{ConfigError, Error};
use crate::utils::spi::{
    CallContext, CircuitBreaker, CredentialsResolver, Lease, Protocol, RetryPolicy, Target,
};

/// How much of an error response's body goes into [`Error::Http`].
const ERROR_BODY_SNIPPET: usize = 1024;

/// Headers the HTTP client manages itself; setting them on a request is an error.
const CONNECTION_HEADERS: [&str; 5] = [
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "upgrade",
];

/// Calls one REST connection.
///
/// Every request goes through [`Policies::run`]: the breaker, a pooled connection, the
/// attempt within `request_timeout`, retries for idempotent methods. It sends the credentials
/// the connection's auth mode allows for the call's [`CallContext`], the trace context and the
/// initiator, and the connection's fixed `headers`.
///
/// Clones share the same connection.
#[derive(Clone)]
pub struct RestClient(Arc<Inner>);

#[derive(Clone)]
struct Inner {
    target: Target,
    base: String,
    base_path: String,
    headers: HeaderMap,
    pool: TypedPool<RestConnector>,
    auth: Arc<dyn CredentialsResolver>,
    retry: Arc<dyn RetryPolicy>,
    breaker: Arc<dyn CircuitBreaker>,
    request_timeout: Duration,
    max_response_size: u64,
}

impl fmt::Debug for RestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestClient")
            .field("connection", &self.0.target.name())
            .field("base", &self.0.base)
            .finish_non_exhaustive()
    }
}

impl RestClient {
    /// A client for the REST connection `settings`, taking connections from `pool` (a handle
    /// over a [`RestConnector`] with this connection registered), credentials from `auth` and
    /// breaker state from `breaker`. Retries follow the connection's `retry` settings
    /// ([`ExponentialBackoff`]); see [`with_retry`](Self::with_retry).
    ///
    /// # Errors
    ///
    /// [`Error::WrongProtocol`] for a gRPC connection; [`Error::Config`] if a fixed header
    /// cannot be sent.
    pub fn new(
        settings: &ConnectionSettings,
        pool: TypedPool<RestConnector>,
        auth: Arc<dyn CredentialsResolver>,
        breaker: Arc<dyn CircuitBreaker>,
    ) -> Result<Self, Error> {
        if settings.protocol != Protocol::Rest {
            return Err(Error::WrongProtocol {
                connection: settings.name.clone(),
                expected: Protocol::Rest,
                actual: settings.protocol,
            });
        }
        let mut headers = HeaderMap::with_capacity(settings.headers.len());
        for (name, value) in &settings.headers {
            let parsed = HeaderName::from_bytes(name.as_bytes())
                .ok()
                .zip(HeaderValue::from_str(value).ok());
            let Some((name, value)) = parsed else {
                return Err(ConfigError::new(
                    format!("connections.{}.headers.{name}", settings.name),
                    "cannot be sent as a header",
                )
                .into());
            };
            headers.append(name, value);
        }
        Ok(Self(Arc::new(Inner {
            target: settings.target(),
            base: settings.base_url(),
            base_path: settings.endpoint.base_path.clone(),
            headers,
            pool,
            auth,
            retry: Arc::new(ExponentialBackoff::for_connection(settings)),
            breaker,
            request_timeout: settings.request_timeout,
            max_response_size: settings.max_response_size,
        })))
    }

    /// Replaces the retry policy.
    pub fn with_retry(mut self, retry: impl RetryPolicy) -> Self {
        Arc::make_mut(&mut self.0).retry = Arc::new(retry);
        self
    }

    /// The connection this client calls.
    pub fn target(&self) -> &Target {
        &self.0.target
    }

    /// A request with `method` to `path` (below the endpoint's base path, starting with `/`).
    ///
    /// GET, HEAD, PUT, DELETE, OPTIONS and TRACE are idempotent, so they are retried; POST and
    /// PATCH are not, unless marked with [`RestRequest::idempotent`].
    pub fn request(&self, method: Method, path: &str) -> RestRequest {
        let idempotent = matches!(
            method,
            Method::GET
                | Method::HEAD
                | Method::PUT
                | Method::DELETE
                | Method::OPTIONS
                | Method::TRACE
        );
        RestRequest {
            client: self.clone(),
            method,
            path: path.to_owned(),
            query: Vec::new(),
            headers: HeaderMap::new(),
            body: None,
            idempotent,
            error: check_path(path).err(),
        }
    }

    /// A GET request.
    pub fn get(&self, path: &str) -> RestRequest {
        self.request(Method::GET, path)
    }

    /// A POST request.
    pub fn post(&self, path: &str) -> RestRequest {
        self.request(Method::POST, path)
    }

    /// A PUT request.
    pub fn put(&self, path: &str) -> RestRequest {
        self.request(Method::PUT, path)
    }

    /// A PATCH request.
    pub fn patch(&self, path: &str) -> RestRequest {
        self.request(Method::PATCH, path)
    }

    /// A DELETE request.
    pub fn delete(&self, path: &str) -> RestRequest {
        self.request(Method::DELETE, path)
    }

    /// A HEAD request.
    pub fn head(&self, path: &str) -> RestRequest {
        self.request(Method::HEAD, path)
    }
}

impl Inner {
    /// The full URL of `path` with `query`, kept below the base path.
    fn url(&self, path: &str, query: &[(String, String)]) -> Result<Url, String> {
        let mut url = Url::parse(&format!("{}{path}", self.base))
            .map_err(|e| format!("invalid path `{path}`: {e}"))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        if !url.path().starts_with(&self.base_path) {
            return Err(format!("path `{path}` leaves the base path"));
        }
        Ok(url)
    }
}

/// A path must start with `/`, hold only visible ASCII, carry no query or fragment (use
/// [`RestRequest::query`]) and no `.`/`..` segment, so it can never leave the base path.
fn check_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/') {
        return Err(format!("path `{path}` must start with `/`"));
    }
    if !path.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(
            "the path may only hold visible ASCII characters (percent-encode the rest)".into(),
        );
    }
    if path.contains(['?', '#']) {
        return Err(format!(
            "path `{path}` holds `?` or `#`; add query parameters with `query`"
        ));
    }
    let dot_segment = path.split('/').any(|segment| {
        let s = segment.to_ascii_lowercase().replace("%2e", ".");
        s == "." || s == ".."
    });
    if dot_segment {
        return Err(format!("path `{path}` holds a `.` or `..` segment"));
    }
    Ok(())
}

/// One request being built. Problems found while building (an invalid path or header, a body
/// that cannot be encoded) are reported by [`send`](Self::send) as [`Error::InvalidRequest`],
/// before anything is sent.
pub struct RestRequest {
    client: RestClient,
    method: Method,
    path: String,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    body: Option<Bytes>,
    idempotent: bool,
    error: Option<String>,
}

impl fmt::Debug for RestRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestRequest")
            .field("connection", &self.client.0.target.name())
            .field("method", &self.method)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RestRequest {
    /// Adds a query parameter (encoded as needed).
    pub fn query(mut self, key: &str, value: impl ToString) -> Self {
        self.query.push((key.to_owned(), value.to_string()));
        self
    }

    /// Adds a header. The credentials, trace headers (`authorization`, `traceparent`,
    /// `tracestate`, `x-request-id`, `x-initiator`) and connection-level headers (`host`,
    /// `content-length`, …) cannot be set here.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        let lower = name.to_ascii_lowercase();
        if RESERVED_HEADERS.contains(&lower.as_str())
            || CONNECTION_HEADERS.contains(&lower.as_str())
        {
            self.fail(format!("header `{name}` is set by z-request"));
            return self;
        }
        match (
            HeaderName::from_bytes(lower.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            (Ok(name), Ok(value)) => {
                self.headers.append(name, value);
            }
            _ => self.fail(format!("header `{name}` cannot be sent as given")),
        }
        self
    }

    /// Sends `body` as JSON (`content-type: application/json`, unless set).
    pub fn json<T: Serialize + ?Sized>(mut self, body: &T) -> Self {
        match serde_json::to_vec(body) {
            Ok(bytes) => {
                self.body = Some(bytes.into());
                self.default_header(CONTENT_TYPE, "application/json");
            }
            Err(e) => self.fail(format!("cannot encode the body as JSON: {e}")),
        }
        self
    }

    /// Sends `body` as is, with `content_type`.
    pub fn body(mut self, content_type: &str, body: impl Into<Bytes>) -> Self {
        match HeaderValue::from_str(content_type) {
            Ok(value) => {
                self.headers.insert(CONTENT_TYPE, value);
                self.body = Some(body.into());
            }
            Err(_) => self.fail(format!("content type `{content_type}` cannot be sent")),
        }
        self
    }

    /// Marks the request as safe to retry (or not), overriding the method's default.
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    /// Sends the request and reads the whole response (up to `max_response_size`).
    ///
    /// # Errors
    ///
    /// [`Error::Http`] for a non-2xx status (redirects are not followed);
    /// [`Error::Decode`] for a body larger than `max_response_size`; the credentials, pool,
    /// breaker, timeout and transport errors of [`Policies::run`].
    pub async fn send(self, ctx: &CallContext) -> Result<RestResponse, Error> {
        let inner = &*self.client.0;
        let connection = inner.target.name();
        let invalid = |reason: String| Error::InvalidRequest {
            connection: connection.to_owned(),
            reason,
        };
        if let Some(reason) = self.error {
            return Err(invalid(reason));
        }
        let url = inner.url(&self.path, &self.query).map_err(invalid)?;
        let sent = inner.auth.resolve(&inner.target, ctx)?;

        let mut headers = inner.headers.clone();
        headers.extend(self.headers);
        for (name, value) in outbound_headers(&sent, ctx.trace()) {
            if let Ok(mut value) = HeaderValue::from_str(&value) {
                value.set_sensitive(name == AUTHORIZATION);
                headers.insert(HeaderName::from_static(name), value);
            }
        }

        let policies = Policies {
            retry: &*inner.retry,
            breaker: &*inner.breaker,
            request_timeout: inner.request_timeout,
        };
        let (method, body, limit) = (&self.method, &self.body, inner.max_response_size);
        policies
            .run(&inner.pool, &inner.target, self.idempotent, |lease, _| {
                let mut request = lease
                    .client()
                    .request(method.clone(), url.clone())
                    .headers(headers.clone());
                if let Some(body) = body {
                    request = request.body(body.clone());
                }
                exchange(lease, request, limit, connection)
            })
            .await
    }

    /// Sends the request (with `accept: application/json`, unless set) and decodes the JSON
    /// response body.
    pub async fn send_json<T: DeserializeOwned>(mut self, ctx: &CallContext) -> Result<T, Error> {
        self.default_header(reqwest::header::ACCEPT, "application/json");
        self.send(ctx).await?.json()
    }

    fn default_header(&mut self, name: HeaderName, value: &'static str) {
        if !self.headers.contains_key(&name) {
            self.headers.insert(name, HeaderValue::from_static(value));
        }
    }

    fn fail(&mut self, reason: String) {
        self.error.get_or_insert(reason);
    }
}

/// One attempt: send, read the body within the limit, map the status.
async fn exchange(
    lease: Lease<RestConnection>,
    request: reqwest::RequestBuilder,
    limit: u64,
    connection: &str,
) -> Result<RestResponse, Error> {
    let result = async {
        let mut response = request.send().await.map_err(|e| transport(connection, e))?;
        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let body = read_body(&mut response, limit, connection).await?;
        if !status.is_success() {
            return Err(Error::Http {
                connection: connection.to_owned(),
                status: status.as_u16(),
                body: snippet(&body),
            });
        }
        Ok(RestResponse {
            connection: connection.to_owned(),
            status,
            headers,
            body,
        })
    }
    .await;
    match &result {
        // The socket failed: close this connection rather than reuse it.
        Err(Error::Connect { .. } | Error::Transport { .. }) => lease.discard(),
        _ => drop(lease),
    }
    result
}

async fn read_body(
    response: &mut reqwest::Response,
    limit: u64,
    connection: &str,
) -> Result<Bytes, Error> {
    let too_large = || Error::Decode {
        connection: connection.to_owned(),
        reason: format!("the response body is larger than the {limit}-byte limit"),
    };
    let expected = response.content_length().unwrap_or(0);
    if expected > limit {
        return Err(too_large());
    }
    let mut body = BytesMut::with_capacity(usize::try_from(expected).unwrap_or(0));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| transport(connection, e))?
    {
        if (body.len() + chunk.len()) as u64 > limit {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// Maps a reqwest error: a failed connect (nothing sent), an invalid request, or a transport
/// failure. The URL is left out of the message.
fn transport(connection: &str, e: reqwest::Error) -> Error {
    let e = e.without_url();
    let connection = connection.to_owned();
    let reason = error_text(&e);
    if e.is_connect() {
        Error::Connect { connection, reason }
    } else if e.is_builder() {
        Error::InvalidRequest { connection, reason }
    } else {
        Error::Transport { connection, reason }
    }
}

/// The start of a body, as text, for an error message.
fn snippet(body: &[u8]) -> String {
    let end = body.len().min(ERROR_BODY_SNIPPET);
    let mut text = String::from_utf8_lossy(body.get(..end).unwrap_or_default()).into_owned();
    if body.len() > end {
        text.push('…');
    }
    text
}

/// A successful (2xx) response, read in full.
#[derive(Debug, Clone)]
pub struct RestResponse {
    connection: String,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl RestResponse {
    /// The status code.
    pub fn status(&self) -> u16 {
        self.status.as_u16()
    }

    /// The response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The body, without copying.
    pub fn into_body(self) -> Bytes {
        self.body
    }

    /// The body as UTF-8 text.
    pub fn text(&self) -> Result<&str, Error> {
        std::str::from_utf8(&self.body).map_err(|e| self.decode_error(e))
    }

    /// The body decoded from JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, Error> {
        serde_json::from_slice(&self.body).map_err(|e| self.decode_error(e))
    }

    fn decode_error(&self, e: impl fmt::Display) -> Error {
        Error::Decode {
            connection: self.connection.clone(),
            reason: e.to_string(),
        }
    }
}

#[cfg(test)]
mod path_tests {
    use super::check_path;

    #[test]
    fn paths_stay_below_the_base_path() {
        for ok in [
            "/",
            "/users",
            "/users/42/orders",
            "/a%20b",
            "/v1.2/x",
            "/..x/y",
        ] {
            assert!(check_path(ok).is_ok(), "{ok}");
        }
        for bad in [
            "users",
            "/../admin",
            "/a/./b",
            "/a/..",
            "/%2e%2e/admin",
            "/%2E./x",
            "/a?b=1",
            "/a#f",
            "/a b",
            "/ünï",
            "http://evil/x",
        ] {
            assert!(check_path(bad).is_err(), "{bad}");
        }
    }
}
