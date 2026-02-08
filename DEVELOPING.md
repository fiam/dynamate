# Developing

This is the developer documentation entry point for `dynamate`.

## Setup

- Use the pinned stable toolchain in [`rust-toolchain.toml`](rust-toolchain.toml) (currently `1.93.0`).
- Install Docker if you want to run integration tests that start DynamoDB Local.

## Required local checks

Before opening a PR:

```bash
cargo fmt --all --check
cargo check --all-targets --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
```

## Local Docker workflow

Use [`compose.yaml`](compose.yaml) for a local DynamoDB + Dynamate setup.

Run Dynamate with `ttyd` and DynamoDB Local:

```bash
docker compose --profile dynamate up --build
```

Then open `http://localhost:7681`.

Seed sample data:

```bash
docker compose --profile seed run --rm seed
```

The seed container uses defaults from `compose.yaml`:

- `DYNAMO_ENDPOINT=http://dynamodb:8000`
- `DYNAMO_TABLE=dyno-music`
- local AWS credentials/region placeholders for DynamoDB Local

You can override them when running seed, for example:

```bash
DYNAMO_TABLE=my-table docker compose --profile seed run --rm seed
```

## Docs index

- CI policy and workflow details: [`docs/CI.md`](docs/CI.md)
- Release process and Homebrew publishing: [`docs/RELEASING.md`](docs/RELEASING.md)
