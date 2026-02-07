<p align="center">
  <img src="assets/logo.svg" alt="dynamate logo" width="720" />
</p>

# dynamate

`dynamate` is your DynamoDB table mate in the terminal, built with Rust 2024.

## What you get

- Fast TUI workflows for DynamoDB tables and items.
- CLI subcommands like `list-tables` and `create-table`.
- Cross-platform release binaries.
- Homebrew distribution for macOS.
- Docker image published to GHCR.

## Quick start

Run locally:

```bash
cargo run -- --help
cargo run -- list-tables --json
```

## Create Table (CLI)

Command examples:

```bash
# minimal
./target/debug/dynamate create-table --table demo --pk PK:S

# with sort key
./target/debug/dynamate create-table --table demo --pk PK:S --sk SK:S

# with indexes
./target/debug/dynamate create-table \
  --table demo \
  --pk PK:S \
  --sk SK:S \
  --gsi GSI1:GSI1PK:S \
  --gsi GSI2:GSI2PK:N:GSI2SK:S:include=owner,status \
  --lsi LSI1:LSI1SK:S:keys_only
```

Syntax rules:

1. `--pk NAME:TYPE` is required. `TYPE` is `S`, `N`, or `B`.
2. `--sk NAME:TYPE` is optional.
3. `--gsi NAME:PK:PK_TYPE[:SK:SK_TYPE][:PROJECTION]` can be repeated.
4. `--lsi NAME:SK:SK_TYPE[:PROJECTION]` can be repeated and requires a table sort key.
5. `PROJECTION` tokens are `all`, `keys_only`, or `include=attr1,attr2`.

## Installation

Install via Homebrew:

```bash
GH_OWNER=fiam
HOMEBREW_TAP_REPO_NAME=homebrew-dynamate
brew tap "${GH_OWNER}/${HOMEBREW_TAP_REPO_NAME}"
brew install dynamate
```

Or download your platform archive from GitHub Releases and place `dynamate` on your `PATH`.

## Developer docs

Engineering and release documentation lives in:

- `DEVELOPING.md`
