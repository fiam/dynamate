# AGENTS.md

## Commit Policy
Use Conventional Commits: `type(scope?): subject` (imperative, lowercase). Prefer types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`, `ci`, `perf`.

Before committing, run and fix all issues:
- `cargo check --all-targets --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`

Add or update tests where appropriate to cover new behavior or bug fixes.

## Code Quality And Operations
- Keep `Cargo.lock` up to date whenever dependencies change.
- Avoid `unwrap`/`expect` in production paths; use error propagation or structured errors instead.
- Log all AWS/DynamoDB interactions at `trace` level, including their duration. On failure, log the error with the same context.

## Rust Toolchain And Modern Features
- Target the latest stable Rust. No web search needed: go directly to https://releases.rs/docs/<version>/ for the current stable version, its release date, and changelog. As of 2026-02-07, stable is 1.93.0 (released 2026-01-22).
- This repo uses the Rust 2024 edition (see Cargo.toml). Keep code compatible with the 2024 edition and prefer its idioms.
- Before starting work, read the changelog for each release after June 2024 (1.80.0+) at https://releases.rs/docs/<version>/. Use newly stabilized language and library features when they improve clarity or safety, and update the list below when adopting new patterns.

## Post-1.79 Highlights (Non-Exhaustive)
- Rust 2024 edition and async closures are stable (1.85).
- `let`-chains in the 2024 edition for multi-condition `if`/`while` (1.88).
- `#[expect]` for lint management with an audit trail (1.81).
- `&raw const`/`&raw mut` operators and `unsafe extern` blocks for FFI (1.82).
- Explicit const-argument inference for generics (1.89).
