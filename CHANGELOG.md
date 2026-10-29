# Changelog

## [Unreleased]

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

[Unreleased]: https://github.com/eugenezimin/z-request/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/eugenezimin/z-request/releases/tag/v0.1.0
