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

> **Status: 0.2.0, all 6 build steps in place:** configuration, the shared
> pool, retries and circuit breaking, credentials, the REST and gRPC transports, the
> `Connections` composition root, and telemetry. A tokio runtime with time enabled is required.

## Usage

```rust,ignore
use z_request::config::ConfigSource;
use z_request::{CallContext, Connections, Initiator, TraceContext};

// At startup (after installing your tracing subscriber and metrics recorder):
let resolved = ConfigSource::new()
    .yaml_file("config/clients.yaml")?
    .with_env("COUNTERPARTIES")
    .load()?
    .resolve()?;
let connections = Connections::build(&resolved)?;

// Per call, with the inbound request's token and trace context:
let ctx = CallContext::user(token, TraceContext {
    traceparent, tracestate, request_id,
    initiator: Some(Initiator::user(&user_id)?),
});

// REST
let entry: Entry = connections.rest("registry")?.get("/entries/42").send_json(&ctx).await?;

// gRPC, with any generated client
let user = connections
    .grpc("users")?
    .call(&ctx, true, |channel| async move {
        UsersClient::new(channel).get_user(GetUser { id: 42 }).await
    })
    .await?
    .into_inner();

// A dedicated channel for a long-lived stream (e.g. z-event's subscriber)
let channel = connections.grpc("events")?.dedicated(&ctx).await?;

// On shutdown
connections.close().await;
```

## Telemetry

Through the `tracing` and `metrics` facades only; install whatever subscriber and recorder you
use (before `Connections::build`), or nothing. Every call runs in a `z_request.call` span and is
counted in `z_request_calls_total`, `z_request_call_duration_seconds` and
`z_request_retries_total`; the pool reports `z_request_pool_connections` and
`z_request_pool_waiting`. Labels are bounded (`connection`, `protocol`, `result`, `auth`,
`state`); credentials are never recorded. See the `telemetry` module for the details.

## Features

| Feature | Gives |
|---|---|
| `rest` | REST transport (reqwest, HTTP/1.1) |
| `grpc` | gRPC transport (tonic, HTTP/2) |
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
