# Configuration

z-request reads one configuration with three sections: `pool`, `defaults` and `connections`.
It comes from a YAML file, env vars, or both (env wins), or from the host's own configuration
system (embed `ZRequestConfig`; it is a plain serde type).

```rust
use z_request::config::ConfigSource;

let resolved = ConfigSource::new()
    .yaml_file("config/clients.yaml")?   // feature `yaml`
    .with_env("COUNTERPARTIES")          // COUNTERPARTIES__POOL__HOLD=15s, …
    .load()?                             // structure: keys, types, durations
    .resolve()?;                         // rules, defaults applied, secrets read
# Ok::<(), z_request::ConfigError>(())
```

Both steps report **every** problem at once, one per line, with the path of the setting.

The complete example is [`z-request.example.yaml`](../z-request.example.yaml).

## `pool`: one pool shared by all connections

| Field | Default | Meaning |
|---|---|---|
| `scope` | `per_uri` | How `min`/`max` count. `per_uri`: each connection has its own `[min, max]`. `total`: `pool.min`/`pool.max` are the floor and cap across all connections together. |
| `min` | `0` | `per_uri`: connections kept open per connection. `total`: kept open across all connections. Opened on demand, not up front; once open they stay open even when idle, whatever `hold` is. |
| `max` | `8` | `per_uri`: most connections per connection. `total`: most across all connections. A number or `unlimited`. |
| `hold` | `30s` | How long a released connection stays open for the next request. **0s to 60s, strictly.** `0s` connects for every request, except that a released connection still goes straight to a request already waiting for it (so load doesn't cause a connect storm). An expired connection closes up to 100 ms late, never early. |
| `acquire_timeout` | `2s` | How long a request waits for a free connection before failing with `AcquireTimeout`. Opening a new connection is bounded separately, by the connection's `connect_timeout`. |

Waiting requests are served first come, first served. Under `total`, when the cap is reached and
a connection needs room, the pool closes the connection that has been idle longest on another
connection, never one needed for that connection's own `min`; if there is none, the request
waits.

## `defaults`: what every connection inherits

| Field | Default | Meaning |
|---|---|---|
| `connect_timeout` | `3s` | Time allowed to open a connection, including TLS. |
| `request_timeout` | `5s` | Deadline for one attempt (sent as `grpc-timeout` for gRPC). For REST it covers sending the request and reading the whole response. |
| `max_response_size` | `10MiB` | REST only: the largest response body accepted; a larger one fails the call with `Decode`. A size with a unit: `512KiB`, `10MiB`, `5MB`. |
| `retry.max_attempts` | `3` | Total attempts counting the first; `1` never retries. Only retryable errors are retried, and only for idempotent calls, except a failed connect: the request was never sent, so any call is retried. |
| `retry.backoff.initial` / `max` | `100ms` / `2s` | First wait, and the cap on any wait. |
| `retry.backoff.multiplier` | `2.0` | Each wait is the previous one times this (at least `1.0`). |
| `retry.backoff.jitter` | `true` | Shorten each wait randomly to 50–100 %. |
| `breaker.failures` | `5` | Consecutive failures that open the breaker; calls then fail fast with `CircuitOpen`. A failure is the peer's: connect failures, timeouts, broken connections, HTTP 5xx and 429, gRPC `UNKNOWN`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS`. Other answers count as successes; local errors (no free pooled connection, credentials) count as neither. |
| `breaker.open_for` | `30s` | How long it stays open before one trial call. The trial's success closes it, its failure opens it again; a trial that never reports back is replaced after another `open_for`. |
| `auth` | `public` | What calls send; see [Auth](#auth). |

## `connections.<name>`: one peer each

The name (lower-case letters, digits, `_`, starting with a letter) is what code uses, e.g.
`connections.grpc("users")`, and the `connection` label on metrics.

| Field | Required | Meaning |
|---|---|---|
| `protocol` | yes | `grpc` or `rest`. The matching feature must be compiled in. |
| `endpoint` | yes | `http(s)://host[:port]`; for REST also a base path (`https://host/api`). gRPC endpoints have no path. |
| `min`, `max` | no | This connection's own limits, always per connection. Under `per_uri` they replace `pool.min`/`pool.max`; under `total` they bound this connection inside the pool total (default `0` / `unlimited`). |
| `connect_timeout`, `request_timeout`, `max_response_size` | no | Override `defaults`. |
| `retry`, `breaker` | no | Override `defaults` field by field (e.g. only `retry.max_attempts`). |
| `auth` | no | Overrides `defaults.auth`. |
| `tls.ca_file` | no | Extra CA certificate to trust (PEM). `https://` endpoints only. |
| `tls.client_cert_file`, `tls.client_key_file` | no, as a pair | Client certificate for mTLS. |
| `tls.server_name` | no | Name expected on the server certificate, if it differs from the host. The connection still goes to the endpoint's host; the name is also sent as SNI and, for REST, as `Host`. |
| `headers` | no | REST only: fixed headers on every request. `authorization`, `traceparent`, `tracestate`, `x-request-id` and `x-initiator` are reserved. |

Each connection counts as one URI for the limits; two connections with the same endpoint are
counted separately.

## Auth

| Mode | Written as | Sends |
|---|---|---|
| `public` | `auth: public` | nothing |
| `user` | `auth: user` | the caller's own token (`authorization: Bearer …`); a call without one fails with `MissingUserCredentials` |
| `env` | `auth: { mode: env, envvar: NAME }` | the service secret from env var `NAME` |
| `mixed` | `auth: { mode: mixed, envvar: NAME }` | the caller's token when the call has one, otherwise the secret |

`mixed` falls back to the secret only when the call has **no** user token (startup loads,
background jobs). It never retries with the secret after the peer rejects the user's token
(401/403): that would bypass the user's own authorization.

Secrets are read from the named env var when the configuration is resolved, never from YAML,
and never printed (`Debug` shows `<redacted>`). A secret must be visible ASCII (no spaces or line
breaks inside) so it can be sent in a header. Likewise a caller's token that is empty or not
visible ASCII fails the call with `InvalidCredentials`; under `mixed` too, it never falls back to
the secret.

### Initiator

Whatever the mode, a call also sends `x-initiator: user:<id>` or `x-initiator: service:<name>`
when its `CallContext` has an `Initiator`. It says on whose behalf an authenticated service calls,
so a system that authenticates every call with a service secret (`env`) can still trace each
request to the user or job that started it. It is **for tracing and audit only**: the peer cannot
verify it and must never base an access decision on it. Use opaque ids, never e-mails or names.
A received value is forwarded with `Initiator::parse`.

## Env vars

`<PREFIX>__` + the YAML path joined by `__`, upper case. Values that look like numbers or
booleans are read as such. Examples with the prefix `COUNTERPARTIES`:

| Env var | Same as |
|---|---|
| `COUNTERPARTIES__POOL__SCOPE=total` | `pool.scope` |
| `COUNTERPARTIES__POOL__HOLD=15s` | `pool.hold` |
| `COUNTERPARTIES__DEFAULTS__RETRY__MAX_ATTEMPTS=1` | `defaults.retry.max_attempts` |
| `COUNTERPARTIES__CONNECTIONS__USERS__ENDPOINT=https://…` | `connections.users.endpoint` |
| `COUNTERPARTIES__CONNECTIONS__USERS__AUTH=user` | `connections.users.auth` (short form) |
| `COUNTERPARTIES__CONNECTIONS__USERS__AUTH__MODE=env` + `…__AUTH__ENVVAR=NAME` | the long form |

A connection can be defined from env vars alone.

## REST

Each pooled REST connection is one HTTP/1.1 socket, so `pool.min`, `max` and `hold` count real
sockets. Redirects are not followed (a 3xx fails the call with `Http`, so a redirect can never
carry the caller's token elsewhere), proxy settings (`HTTP_PROXY`, …) are ignored, and HTTPS
servers are verified against the platform's trust store plus `tls.ca_file`.

A request path is joined to the endpoint's base path and can never leave it: it must start with
`/`, and `.`/`..` segments (also percent-encoded) are rejected.

## gRPC

Each pooled gRPC connection is one HTTP/2 socket, and a call holds it on its own, as a REST
request does: `max` is both the sockets and the concurrent calls to a peer, so give busy gRPC
peers a higher `max` than REST ones. `request_timeout` is also sent as `grpc-timeout`. With
`tls.server_name`, the certificate is checked for that name (also sent as SNI); `:authority`
stays the endpoint's host.

A long-lived stream (a subscription) takes a dedicated connection that stays out of the pool,
and counts against `max`, until the stream's holder drops it.

## Sizes

Text with a unit: `512B`, `64KiB`, `10MiB`, `1GiB` (binary) or `10KB`, `5MB`, `1GB` (decimal). A
bare number (`1024`) is rejected.

## Durations

Text with a unit: `100ms`, `2s`, `5m`, `1h`, `1m30s`. A bare number (`30`) is rejected.

## Checks

Structural (`load`): unknown sections or keys, wrong types, invalid durations, malformed `auth`.

Semantic (`resolve`):
- `pool.hold` outside 0–60 s; zero timeouts or `max_response_size`; `min > max` (pool or connection);
- under `total`, the connections' own `min` adding up to more than `pool.max`;
- connection names, missing `protocol`/`endpoint`, malformed endpoints, a protocol or `https`
  without its feature, a path on a gRPC endpoint;
- `retry` (`max_attempts ≥ 1`, `initial ≤ max`, `multiplier ≥ 1.0`), `breaker.failures ≥ 1`;
- `env`/`mixed` with an `envvar` that is invalid, unset or empty, or a secret that is not visible
  ASCII;
- TLS on an `http://` endpoint, half an mTLS pair;
- `headers` on gRPC, reserved or invalid header names, multi-line values.
