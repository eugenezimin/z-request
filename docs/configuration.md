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
| `min` | `0` | `per_uri`: connections kept open per connection. `total`: kept open across all connections. They stay open even when idle, whatever `hold` is. |
| `max` | `8` | `per_uri`: most connections per connection. `total`: most across all connections. A number or `unlimited`. |
| `hold` | `30s` | How long a released connection stays open for the next request. **0s to 60s, strictly.** `0s` connects for every request. |
| `acquire_timeout` | `2s` | How long a request waits for a free connection before failing with `AcquireTimeout`. |

## `defaults`: what every connection inherits

| Field | Default | Meaning |
|---|---|---|
| `connect_timeout` | `3s` | Time allowed to open a connection, including TLS. |
| `request_timeout` | `5s` | Deadline for one attempt (sent as `grpc-timeout` for gRPC). |
| `retry.max_attempts` | `3` | Total attempts counting the first; `1` never retries. Only idempotent calls are retried, and only on retryable errors. |
| `retry.backoff.initial` / `max` | `100ms` / `2s` | First wait, and the cap on any wait. |
| `retry.backoff.multiplier` | `2.0` | Each wait is the previous one times this (at least `1.0`). |
| `retry.backoff.jitter` | `true` | Shorten each wait randomly to 50–100 %. |
| `breaker.failures` | `5` | Consecutive failures that open the breaker; calls then fail fast with `CircuitOpen`. |
| `breaker.open_for` | `30s` | How long it stays open before one trial call. |
| `auth` | `public` | What calls send; see [Auth](#auth). |

## `connections.<name>`: one peer each

The name (lower-case letters, digits, `_`, starting with a letter) is what code uses, e.g.
`connections.grpc("users")`, and the `connection` label on metrics.

| Field | Required | Meaning |
|---|---|---|
| `protocol` | yes | `grpc` or `rest`. The matching feature must be compiled in. |
| `endpoint` | yes | `http(s)://host[:port]`; for REST also a base path (`https://host/api`). gRPC endpoints have no path. |
| `min`, `max` | no | This connection's own limits, always per connection. Under `per_uri` they replace `pool.min`/`pool.max`; under `total` they bound this connection inside the pool total (default `0` / `unlimited`). |
| `connect_timeout`, `request_timeout` | no | Override `defaults`. |
| `retry`, `breaker` | no | Override `defaults` field by field (e.g. only `retry.max_attempts`). |
| `auth` | no | Overrides `defaults.auth`. |
| `tls.ca_file` | no | Extra CA certificate to trust (PEM). `https://` endpoints only. |
| `tls.client_cert_file`, `tls.client_key_file` | no, as a pair | Client certificate for mTLS. |
| `tls.server_name` | no | Name expected on the server certificate, if it differs from the host. |
| `headers` | no | REST only: fixed headers on every request. `authorization`, `traceparent`, `tracestate` and `x-request-id` are reserved. |

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
and never printed (`Debug` shows `<redacted>`).

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

## Durations

Text with a unit: `100ms`, `2s`, `5m`, `1h`, `1m30s`. A bare number (`30`) is rejected.

## Checks

Structural (`load`): unknown sections or keys, wrong types, invalid durations, malformed `auth`.

Semantic (`resolve`):
- `pool.hold` outside 0–60 s; zero timeouts; `min > max` (pool or connection);
- under `total`, the connections' own `min` adding up to more than `pool.max`;
- connection names, missing `protocol`/`endpoint`, malformed endpoints, a protocol or `https`
  without its feature, a path on a gRPC endpoint;
- `retry` (`max_attempts ≥ 1`, `initial ≤ max`, `multiplier ≥ 1.0`), `breaker.failures ≥ 1`;
- `env`/`mixed` with an `envvar` that is invalid, unset or empty;
- TLS on an `http://` endpoint, half an mTLS pair;
- `headers` on gRPC, reserved or invalid header names, multi-line values.
