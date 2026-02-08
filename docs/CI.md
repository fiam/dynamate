# CI

## Workflow

- File: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
- Triggers: `pull_request`, `push` to `main` and `master`
- Toolchain: stable Rust `1.93.0`

## Checks performed

1. `cargo fmt --all --check`
2. `cargo check --all-targets --all-features --locked`
3. `cargo clippy --all-targets --all-features --locked -- -D warnings`
4. `cargo test --all-targets --all-features --locked`
5. `cargo doc --all-features --no-deps --locked` with `RUSTDOCFLAGS=-D warnings`
6. `cargo deny check advisories bans licenses sources`
7. `cargo audit`

## Dependency policy config

- File: [`deny.toml`](../deny.toml)
- Enforces known registry sources.
- Denies yanked crates.
- Enables advisory, bans, and license checks.

## Running CI checks locally

```bash
cargo fmt --all --check
cargo check --all-targets --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps --locked
cargo deny check advisories bans licenses sources
cargo audit
```
