# Changelog

## Unreleased

### Added
- Interfaces (`spi`): `Connector`, `Connection`, `Pool`, `Lease`, `EvictionPolicy`, `RetryPolicy`,
  `CircuitBreaker`, `CredentialsResolver`, `ConfigLoader`, and the value types `Target`,
  `CallContext`, `TraceContext`, `Secret`, `Sent`.
- `Error` (with `is_retryable()`) and `ConfigError` (every problem at once).
- Configuration: `ZRequestConfig` (`pool`, `defaults`, `connections`), `ConfigSource` (YAML +
  `<PREFIX>__…` env vars), `ZRequestConfig::resolve` (validation, defaults applied per connection,
  secrets read from env).
