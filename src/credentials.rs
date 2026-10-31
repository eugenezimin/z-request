//! What a call sends: the credentials its connection's auth mode allows, and the headers that
//! carry them and the trace context.
//!
//! [`AuthResolver`] is the default [`CredentialsResolver`]. [`outbound_headers`] turns the
//! result and the call's [`TraceContext`] into the header set both transports send.

use std::borrow::Cow;
use std::collections::HashMap;

use crate::config::{AuthSettings, ConnectionSettings};
use crate::utils::error::Error;
use crate::utils::spi::{
    CallContext, CredentialsResolver, INITIATOR_HEADER, Secret, Sent, Target, TraceContext,
};

/// The header that carries the credentials (`Bearer <token>`).
pub const AUTHORIZATION: &str = "authorization";
/// The W3C trace context header.
pub const TRACEPARENT: &str = "traceparent";
/// The W3C trace state header.
pub const TRACESTATE: &str = "tracestate";
/// The request id header.
pub const REQUEST_ID: &str = "x-request-id";

/// Headers z-request sets itself on every call; configuring or adding them is an error.
pub(crate) const RESERVED_HEADERS: [&str; 5] = [
    AUTHORIZATION,
    TRACEPARENT,
    TRACESTATE,
    REQUEST_ID,
    INITIATOR_HEADER,
];

/// Whether `token` can be sent as a bearer token: 1 or more visible ASCII characters (no
/// spaces, no control characters, nothing outside ASCII).
pub fn is_header_safe(token: &str) -> bool {
    !token.is_empty() && token.bytes().all(|b| b.is_ascii_graphic())
}

/// Whether `value` can be sent as a header value: visible ASCII, spaces and tabs.
fn is_header_value(value: &str) -> bool {
    value
        .bytes()
        .all(|b| b.is_ascii_graphic() || b == b' ' || b == b'\t')
}

/// The default [`CredentialsResolver`]: each connection's configured auth mode.
///
/// | Mode | Sends |
/// |---|---|
/// | `public` | nothing |
/// | `user` | the call's user token; a call without one fails with [`Error::MissingUserCredentials`] |
/// | `env` | the service secret |
/// | `mixed` | the call's user token if it has one, otherwise the service secret |
///
/// A user token that is empty or not visible ASCII fails with [`Error::InvalidCredentials`],
/// so it can never inject a header. Under `mixed` such a token is an error too: falling back to
/// the service secret would bypass the user's own authorization.
#[derive(Debug, Clone, Default)]
pub struct AuthResolver {
    by_target: HashMap<Target, AuthSettings>,
}

impl AuthResolver {
    /// A resolver for `connections`.
    pub fn new<'a>(connections: impl IntoIterator<Item = &'a ConnectionSettings>) -> Self {
        Self {
            by_target: connections
                .into_iter()
                .map(|c| (c.target(), c.auth.clone()))
                .collect(),
        }
    }
}

impl CredentialsResolver for AuthResolver {
    fn resolve(&self, target: &Target, ctx: &CallContext) -> Result<Sent, Error> {
        let auth = self
            .by_target
            .get(target)
            .ok_or_else(|| Error::UnknownConnection {
                connection: target.name().to_owned(),
            })?;
        let user = |token: &Secret| -> Result<Sent, Error> {
            if is_header_safe(token.expose()) {
                Ok(Sent::User(token.clone()))
            } else {
                Err(Error::InvalidCredentials {
                    connection: target.name().to_owned(),
                    reason: if token.expose().is_empty() {
                        "the token is empty".into()
                    } else {
                        "the token may only hold visible ASCII characters".into()
                    },
                })
            }
        };
        match (auth, ctx.user_token()) {
            (AuthSettings::Public, _) => Ok(Sent::None),
            (AuthSettings::User | AuthSettings::Mixed(_), Some(token)) => user(token),
            (AuthSettings::User, None) => Err(Error::MissingUserCredentials {
                connection: target.name().to_owned(),
            }),
            (AuthSettings::Env(secret), _) | (AuthSettings::Mixed(secret), None) => {
                Ok(Sent::Secret(secret.clone()))
            }
        }
    }
}

/// The headers a call sends, in order: `authorization: Bearer …` (if `sent` has a token),
/// `traceparent`, `tracestate`, `x-request-id` and `x-initiator` (those the trace context has).
///
/// A value a header cannot carry (control characters, line breaks, non-ASCII) is left out
/// rather than sent. [`AuthResolver`] and configuration validation already guarantee that
/// credentials are safe; trace values come from the inbound request and are not.
///
/// Only `authorization` is allocated; the rest borrow from `trace`.
pub fn outbound_headers<'a>(
    sent: &'a Sent,
    trace: &'a TraceContext,
) -> impl Iterator<Item = (&'static str, Cow<'a, str>)> + 'a {
    let authorization = sent
        .bearer()
        .filter(|s| is_header_safe(s.expose()))
        .map(|s| (AUTHORIZATION, Cow::Owned(format!("Bearer {}", s.expose()))));
    let traced = [
        (TRACEPARENT, trace.traceparent.as_deref()),
        (TRACESTATE, trace.tracestate.as_deref()),
        (REQUEST_ID, trace.request_id.as_deref()),
        (
            INITIATOR_HEADER,
            trace.initiator.as_ref().map(|i| i.as_str()),
        ),
    ]
    .into_iter()
    .filter_map(|(name, value)| {
        value
            .filter(|v| !v.is_empty() && is_header_value(v))
            .map(|v| (name, Cow::Borrowed(v)))
    });
    authorization.into_iter().chain(traced)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::connection;
    use crate::utils::spi::Initiator;

    fn conn(name: &str, auth: AuthSettings) -> ConnectionSettings {
        ConnectionSettings {
            auth,
            ..connection(name)
        }
    }

    fn resolver() -> (AuthResolver, [Target; 4]) {
        let secret = || Secret::new("service-secret");
        let conns = [
            conn("public", AuthSettings::Public),
            conn("user", AuthSettings::User),
            conn("env", AuthSettings::Env(secret())),
            conn("mixed", AuthSettings::Mixed(secret())),
        ];
        let targets = conns.each_ref().map(ConnectionSettings::target);
        (AuthResolver::new(&conns), targets)
    }

    fn user(token: &str) -> CallContext {
        CallContext::user(token, TraceContext::default())
    }

    fn anonymous() -> CallContext {
        CallContext::without_user(TraceContext::default())
    }

    #[test]
    fn each_mode_sends_what_it_allows() {
        let (r, [public, user_only, env, mixed]) = resolver();
        let secret = Sent::Secret(Secret::new("service-secret"));
        let token = Sent::User(Secret::new("u-token"));

        assert_eq!(r.resolve(&public, &user("u-token")).unwrap(), Sent::None);
        assert_eq!(r.resolve(&user_only, &user("u-token")).unwrap(), token);
        assert!(matches!(
            r.resolve(&user_only, &anonymous()),
            Err(Error::MissingUserCredentials { .. })
        ));
        assert_eq!(r.resolve(&env, &user("u-token")).unwrap(), secret);
        assert_eq!(r.resolve(&env, &anonymous()).unwrap(), secret);
        assert_eq!(r.resolve(&mixed, &user("u-token")).unwrap(), token);
        assert_eq!(r.resolve(&mixed, &anonymous()).unwrap(), secret);
        assert!(matches!(
            r.resolve(&Target::new("nope", "http://nope"), &anonymous()),
            Err(Error::UnknownConnection { .. })
        ));
    }

    #[test]
    fn unsafe_user_tokens_are_rejected_without_being_shown() {
        let (r, [_, user_only, _, mixed]) = resolver();
        for bad in ["", "a b", "tok\r\nx-evil: 1", "tökén"] {
            for target in [&user_only, &mixed] {
                match r.resolve(target, &user(bad)) {
                    Err(e @ Error::InvalidCredentials { .. }) => {
                        assert!(!e.is_retryable());
                        if !bad.is_empty() {
                            assert!(!e.to_string().contains(bad), "{e}");
                        }
                    }
                    other => panic!("{bad:?} on {}: {other:?}", target.name()),
                }
            }
        }
    }

    #[test]
    fn outbound_headers_carry_credentials_and_trace() {
        let trace = TraceContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=a, other=b".into()),
            request_id: Some("req-1".into()),
            initiator: Some(Initiator::user("u1").unwrap()),
        };
        let sent = Sent::User(Secret::new("u-token"));
        let headers: Vec<(&str, String)> = outbound_headers(&sent, &trace)
            .map(|(n, v)| (n, v.into_owned()))
            .collect();
        assert_eq!(
            headers,
            [
                (AUTHORIZATION, "Bearer u-token".to_owned()),
                (TRACEPARENT, trace.traceparent.clone().unwrap()),
                (TRACESTATE, "vendor=a, other=b".to_owned()),
                (REQUEST_ID, "req-1".to_owned()),
                (INITIATOR_HEADER, "user:u1".to_owned()),
            ]
        );
    }

    #[test]
    fn outbound_headers_leave_out_what_cannot_be_sent() {
        let trace = TraceContext {
            traceparent: Some("00-x\r\nx-evil: 1".into()),
            tracestate: Some(String::new()),
            request_id: Some("ok".into()),
            initiator: None,
        };
        let names: Vec<&str> = outbound_headers(&Sent::None, &trace)
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, [REQUEST_ID]);
        let forged = Sent::Secret(Secret::new("bad\nsecret"));
        assert_eq!(
            outbound_headers(&forged, &TraceContext::default()).count(),
            0
        );
    }
}
