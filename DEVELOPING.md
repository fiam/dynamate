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

## Docs index

- CI policy and workflow details: [`docs/CI.md`](docs/CI.md)
- Release process and Homebrew publishing: [`docs/RELEASING.md`](docs/RELEASING.md)
