# z-request

Pooled outbound clients for services: any number of named **connections** to REST and gRPC
peers, configured from YAML and env vars, sending the **end user's own credentials** so the peer
can authorize the user and trace their path. Calls without a user send nothing or a server-side
secret, as configured.

z-request sits next to [z-auth](../z-auth) and [z-event](https://github.com/eugenezimin/z-event)
without depending on either: the host passes the user's z-auth-verified token in a `CallContext`,
and can hand a dedicated gRPC channel to z-event's subscriber.

Every call can also name its **initiator** (`x-initiator: user:<id>` or `service:<name>`), sent
next to whatever credentials the connection uses. Services that authenticate to each other with
their own secrets use it to trace each request back to the user or job that started it; it is
never an authorization input.

> **Status: build step 4 of 6.** Interfaces, errors, configuration, the connection pool, the
> retry/breaker policies with the shared call loop, credentials and the REST transport are in
> place. The gRPC transport follows. A tokio runtime with time enabled is required.

## Features

| Feature | Gives |
|---|---|
| `rest` | REST transport (reqwest, HTTP/1.1) |
| `grpc` | gRPC transport (tonic) |
| `tls` | `https://`, custom CAs, client certificates (rustls with `ring`, platform trust store) |
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
