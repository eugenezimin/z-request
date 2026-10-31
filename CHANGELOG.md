# Changelog

## [Unreleased]

### Added
- `pool`: `ConnectionPool`, the one pool shared by every connection of every protocol
  (`per_uri`/`total` limits, `min` opened on demand and never closed by `hold`, `hold` expiry,
  first-come-first-served waiting within `acquire_timeout`, eviction of another connection's
  idle connection at the `total` cap, `close()` for shutdown); `TypedPool<K>`, the per-protocol
  handle implementing `spi::Pool`; `LruEviction`, the default `EvictionPolicy`; `TargetStats`.
- `Error::PoolClosed`.
- `Initiator` (`user:<id>` / `service:<name>`), carried in `TraceContext` and sent as
  `x-initiator` for tracing and audit; `x-initiator` is a reserved header.

### Dependencies
- `tokio` (features `rt`, `sync`, `time`).

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
