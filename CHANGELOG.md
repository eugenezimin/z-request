# Changelog

## [Unreleased]

## [0.3.0] - 2026-11-01

### Added
- HTTP/2 multiplexing for both transports: pooled request slots share HTTP/2 sockets, up to
  `streams_per_connection` (default `32`) per socket. REST learns from responses whether a peer
  speaks HTTP/2 (`http2: auto`, the default, over `https://`), falls back to a socket per
  request for HTTP/1.1 peers, and goes back to HTTP/1.1 if a peer downgrades; `http2: off` keeps
  HTTP/1.1 only, and `http2: prior_knowledge` speaks HTTP/2 over plain `http://` (h2c). gRPC is
  always HTTP/2. A request past `request_timeout` cancels only its own stream.
- `max_connection_age` (default `5m`, ±10 %, `0s` = off): a socket that old takes no new
  requests, so sockets rotate and load spreads over a peer's replicas behind socket-level load
  balancers. Dedicated gRPC channels are never rotated.
- A dedicated gRPC channel (`GrpcClient::dedicated`) gets a socket of its own.
- Telemetry: the `z_request_sockets{connection, protocol}` gauge and the `http.version` span
  field (REST).
- REST retries HTTP/2 requests the server refused without processing them (`REFUSED_STREAM`,
  `GOAWAY`), even POSTs.

### Changed
- The pool's `min`, `max` and `hold` count request slots, not sockets: `max` is the requests in
  flight to a peer. Over HTTP/1.1 nothing changes; over HTTP/2 the same `max` uses far fewer
  sockets. `z_request_pool_connections` counts slots; `z_request_sockets` counts sockets.
- `DefaultsConfig`, `ConnectionConfig` and `ConnectionSettings` have new public fields
  (`http2`, `streams_per_connection`, `max_connection_age`).

### Dependencies
- reqwest's `http2` feature.

## [0.2.0] - 2026-10-31

### Added
- `Connections`, the composition root: `Connections::build(&resolved)` builds every connection
  (one shared pool, connectors, `AuthResolver`, `ConsecutiveFailures`), hands out
  `rest(name)` / `grpc(name)` clients, and shuts down with `close()`. `ConnectionsBuilder`
  replaces the eviction policy, the credentials resolver or the breaker.
- `pool`: `ConnectionPool`, the one pool shared by every connection of every protocol
  (`per_uri`/`total` limits, `min` opened on demand and never closed by `hold`, `hold` expiry,
  first-come-first-served waiting within `acquire_timeout`, eviction of another connection's
  idle connection at the `total` cap, `close()` for shutdown); `TypedPool<K>`, the per-protocol
  handle implementing `spi::Pool`; `LruEviction`, the default `EvictionPolicy`; `TargetStats`.
- `policy`: `ExponentialBackoff` (the default `RetryPolicy`: exponential waits with jitter,
  idempotent calls only, except that a failed connect is retried for any call),
  `ConsecutiveFailures` (the default `CircuitBreaker`, per connection, with one trial call after
  `open_for`) and `BreakerState`; `outcome()`, what counts against a peer; `Policies::run`, the
  call loop both transports use (breaker, pooled connection, `request_timeout`, retries).
- `credentials`: `AuthResolver` (the default `CredentialsResolver` for `public`, `user`, `env`
  and `mixed`), `outbound_headers()` (authorization, trace context and `x-initiator`, leaving out
  values a header cannot carry) and the header-name constants.
- `rest` (feature `rest`): `RestConnector` and `RestConnection` (one reqwest client per pooled
  connection, keeping one socket, so the pool's limits count real sockets; no redirects, no
  proxies, no retries of its own), and `RestClient` with `RestRequest`/`RestResponse` (credentials,
  trace headers and fixed headers on every request, retries through `Policies::run`, responses
  read in full up to `max_response_size`, paths kept below the base path).
- `grpc` (feature `grpc`): `GrpcConnector` and `GrpcConnection` (one tonic channel, one HTTP/2
  socket, per pooled connection; a call holds it on its own), `GrpcClient::call` (one call through
  `Policies::run` on a `GrpcChannel` that generated clients take as is, adding credentials, trace
  context, initiator and `grpc-timeout`) and `GrpcClient::dedicated` (a `DedicatedChannel` for a
  long-lived stream). tonic's own deadline, unreachable-peer and broken-connection errors map to
  `Timeout`, `Connect` and `Transport`; the peer's statuses to `Status`. `grpc::tonic` re-exports
  the tonic version used.
- HTTPS (feature `tls`) for both transports: rustls with `ring`, servers verified against the
  platform's trust store plus `tls.ca_file`, client certificates for mTLS, and
  `tls.server_name`.
- `telemetry`: a `z_request.call` span per call (connection, protocol, method, path, auth,
  attempts, result, status, error), retry and breaker events, and metrics through the `metrics`
  facade: `z_request_calls_total`, `z_request_call_duration_seconds`, `z_request_retries_total`,
  `z_request_pool_connections`, `z_request_pool_waiting`, with bounded labels. No subscriber,
  recorder or exporter is installed.
- Configuration: `max_response_size` (default `10MiB`, overridable per connection) and the `size`
  format (`512KiB`, `10MiB`, `5MB`).
- Errors: `PoolClosed`, `InvalidCredentials` (a caller's token that is empty or not visible ASCII
  is rejected rather than sent), `InvalidRequest` (a request that cannot be sent as built), and
  `Error::kind()`, a short, stable name for each error (the `result` label).

### Changed
- Configuration: an `env`/`mixed` secret that is not visible ASCII is rejected.
- `DefaultsConfig`, `ConnectionConfig` and `ConnectionSettings` have a new public field,
  `max_response_size`; code that builds them field by field must set it (or use `..Default`).

### Dependencies
- `tokio` (features `rt`, `sync`, `time`).
- `fastrand`, for retry jitter.
- `reqwest` (feature `rest`, no default features), `bytes`; `tonic` (feature `grpc`, `channel`
  only); `rustls` (`ring`) and `rustls-platform-verifier` (feature `tls`, which also enables
  tonic's `tls-ring`).
- `tracing` and `metrics` (facades only).

## [0.1.0] - 2026-10-29

### Added
- Interfaces (`spi`): `Connector`, `Connection`, `Pool`, `Lease`, `EvictionPolicy`, `RetryPolicy`,
  `CircuitBreaker`, `CredentialsResolver`, `ConfigLoader`, and the value types `Target`,
  `CallContext`, `TraceContext`, `Secret`, `Sent`.
- `Initiator` (`user:<id>` / `service:<name>`), carried in `TraceContext` and sent as
  `x-initiator` for tracing and audit; `x-initiator` is a reserved header.
- `Error` (with `is_retryable()`) and `ConfigError` (every problem at once).
- Configuration: `ZRequestConfig` (`pool`, `defaults`, `connections`), `ConfigSource` (YAML +
  `<PREFIX>__…` env vars), `ZRequestConfig::resolve` (validation, defaults applied per connection,
  secrets read from env).

[Unreleased]: https://github.com/eugenezimin/z-request/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/eugenezimin/z-request/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/eugenezimin/z-request/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/eugenezimin/z-request/releases/tag/v0.1.0
