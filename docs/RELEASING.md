# Releasing

## Workflows

- Tag creation workflow: `.github/workflows/create-release.yml`
- Release workflow: `.github/workflows/release.yml`
- The release workflow also builds and pushes a multi-arch Docker image to GHCR.

## Repositories required

1. Main source repository (this repository).
2. Homebrew tap repository, usually `${GH_OWNER}/${HOMEBREW_TAP_REPO_NAME}`.

The tap repository must contain a `Formula/` directory. The release workflow writes `Formula/dynamate.rb`.

## Release constants

Set once in `.github/workflows/release.yml` under top-level `env`:

1. `GH_OWNER=fiam`
2. `HOMEBREW_TAP_REPO_NAME=homebrew-dynamate`

The workflow combines those constants to resolve the tap repository.

## Secrets required in source repository

Set these in `Settings -> Secrets and variables -> Actions`:

1. `HOMEBREW_TAP_TOKEN`
Fine-grained PAT with `Contents: Read and write` access to the tap repository.

No extra secret is needed for GHCR publishing. The workflow uses `GITHUB_TOKEN`.

## Permissions required

- Actions `GITHUB_TOKEN` should have repository read/write permissions.
- Release workflows request `contents: write`.
- Release workflow also requires `packages: write` to publish `ghcr.io` images.

## Release process

1. Update `Cargo.toml` version to a new semver value.
2. Run required checks from `DEVELOPING.md`.
3. Trigger `Create Release Tag` with version `X.Y.Z` and target ref.
4. Workflow validates version and pushes annotated tag `vX.Y.Z`.
5. `Release` workflow starts automatically from the tag.
6. Cross-platform archives are built and uploaded to GitHub Releases.
7. `SHA256SUMS.txt` is generated and uploaded.
8. If Homebrew secrets are present, formula is updated and pushed to the tap repository.
9. A multi-arch Docker image is built from `Dockerfile` and pushed to GHCR.

## Artifacts produced

- `dynamate-<version>-x86_64-unknown-linux-gnu.tar.gz`
- `dynamate-<version>-x86_64-apple-darwin.tar.gz`
- `dynamate-<version>-aarch64-apple-darwin.tar.gz`
- `dynamate-<version>-x86_64-pc-windows-msvc.zip`
- `SHA256SUMS.txt`
- Docker image `ghcr.io/${GH_OWNER}/${GHCR_IMAGE_NAME}:<version>`
- Docker image `ghcr.io/${GH_OWNER}/${GHCR_IMAGE_NAME}:<tag>`
- Docker image `ghcr.io/${GH_OWNER}/${GHCR_IMAGE_NAME}:latest` (stable releases only)

## Manual release trigger

You can run `Release` manually for an existing tag by using the workflow dispatch input `tag`.
