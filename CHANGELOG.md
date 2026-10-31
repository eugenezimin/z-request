# Changelog

## [Unreleased]

### Added
- `pool`: `ConnectionPool`, the one pool shared by every connection of every protocol
  (`per_uri`/`total` limits, `min` opened on demand and never closed by `hold`, `hold` expiry,
  first-come-first-served waiting within `acquire_timeout`, eviction of another connection's
  idle connection at the `total` cap, `close()` for shutdown); `TypedPool<K>`, the per-protocol
  handle implementing `spi::Pool`; `LruEviction`, the default `EvictionPolicy`; `TargetStats`.
- `Error::PoolClosed`.
- `policy`: `ExponentialBackoff` (the default `RetryPolicy`: exponential waits with jitter,
  idempotent calls only, except that a failed connect is retried for any call),
  `ConsecutiveFailures` (the default `CircuitBreaker`, per connection, with one trial call after
  `open_for`) and `BreakerState`; `outcome()`, what counts against a peer; `Policies::run`, the
  call loop both transports use (breaker, pooled connection, `request_timeout`, retries).
- `credentials`: `AuthResolver` (the default `CredentialsResolver` for `public`, `user`, `env`
  and `mixed`), `outbound_headers()` (authorization, trace context and `x-initiator`, leaving out
  values a header cannot carry) and the header-name constants.
- `Error::InvalidCredentials`: a caller's token that is empty or not visible ASCII is rejected
  rather than sent.

### Changed
- Configuration: an `env`/`mixed` secret that is not visible ASCII is rejected.
- `Initiator` (`user:<id>` / `service:<name>`), carried in `TraceContext` and sent as
  `x-initiator` for tracing and audit; `x-initiator` is a reserved header.

### Dependencies
- `tokio` (features `rt`, `sync`, `time`).
- `fastrand`, for retry jitter.

## [0.1.0] - 2026-10-29

### Added
- Interfaces (`spi`): `Connector`, `Connection`, `Pool`, `Lease`, `EvictionPolicy`, `RetryPolicy`,
  `CircuitBreaker`, `CredentialsResolver`, `ConfigLoader`, and the value types `Target`,
  `CallContext`, `TraceContext`, `Secret`, `Sent`.
- `Error` (with `is_retryable()`) and `ConfigError` (every problem at once).
- Configuration: `ZRequestConfig` (`pool`, `defaults`, `connections`), `ConfigSource` (YAML +
  `<PREFIX>__…` env vars), `ZRequestConfig::resolve` (validation, defaults applied per connection,
  secrets read from env).

[Unreleased]: https://github.com/eugenezimin/z-request/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/eugenezimin/z-request/releases/tag/v0.1.0
