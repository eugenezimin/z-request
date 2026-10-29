# z-request

Pooled outbound clients for services: any number of named **connections** to REST and gRPC
peers, configured from YAML and env vars, sending the **end user's own credentials** so the peer
can authorize the user and trace their path. Calls without a user send nothing or a server-side
secret, as configured.

z-request sits next to [z-auth](../z-auth) and [z-event](https://github.com/eugenezimin/z-event)
without depending on either: the host passes the user's z-auth-verified token in a `CallContext`,
and can hand a dedicated gRPC channel to z-event's subscriber.

> **Status: build step 1 of 6.** Interfaces, errors and configuration are in place. The pool,
> retry/breaker policies, credentials, and the REST and gRPC transports follow.

## Features

| Feature | Gives |
|---|---|
| `rest` | REST transport (reqwest) |
| `grpc` | gRPC transport (tonic) |
| `tls` | `https://`, custom CAs, client certificates |
| `yaml` | YAML configuration files |

All four are on by default; `default-features = false` and pick what you need. The core compiles
with none of them.

## Configuration

See [docs/configuration.md](docs/configuration.md) (every field) and
[z-request.example.yaml](z-request.example.yaml).

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo nextest run --all-features && cargo test --doc --all-features
RUSTDOCFLAGS=-D warnings cargo doc --all-features --no-deps
cargo deny check
```

## License

MIT OR Apache-2.0.
