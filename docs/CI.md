# CI

## Workflow

- File: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
- Triggers: `pull_request`, `push` to `main` and `master`
- Toolchain: stable Rust `1.96.0`

## Checks performed

1. `cargo fmt --all --check`
2. `cargo check --all-targets --all-features --locked`
3. `cargo clippy --all-targets --all-features --locked -- -D warnings`
4. `cargo test --all-targets --all-features --locked`
5. `cargo doc --all-features --no-deps --locked` with `RUSTDOCFLAGS=-D warnings`
6. `cargo deny check advisories bans licenses sources`
7. `cargo audit`

## Lint config

- [`Cargo.toml`](../Cargo.toml) `[lints.clippy]`: enables a curated
  `clippy::pedantic` plus `cognitive_complexity`, with an allow-list for
  domain noise.
- [`clippy.toml`](../clippy.toml): function size and complexity thresholds
  (`too-many-lines`, `cognitive-complexity`, `type-complexity`); allows
  `unwrap`/`expect` in tests.
- [`rustfmt.toml`](../rustfmt.toml): pins `edition` and `max_width`.
- These are enforced by the `cargo clippy -- -D warnings` step above; no
  separate CI step is needed.

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
