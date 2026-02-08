# syntax=docker/dockerfile:1.7

FROM rust:1.93-bookworm AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        nasm \
        perl \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target-cache,sharing=locked \
    CARGO_TARGET_DIR=/app/target-cache \
    cargo install --locked --path . --root /app/install --bin dynamate

FROM debian:bookworm-slim

ARG TTYD_VERSION=1.7.7
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN case "$TARGETARCH" in \
        amd64) ttyd_asset="ttyd.x86_64" ;; \
        arm64) ttyd_asset="ttyd.aarch64" ;; \
        *) echo "Unsupported arch: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl -L -o /usr/local/bin/ttyd \
        "https://github.com/tsl0922/ttyd/releases/download/${TTYD_VERSION}/${ttyd_asset}" \
    && chmod +x /usr/local/bin/ttyd

RUN useradd -r -u 10001 -g users -m -d /home/dynamate dynamate

ENV DYNAMATE_DATA=/data \
    TERM=xterm-256color

RUN mkdir -p /data \
    && chown -R dynamate:users /data

COPY --from=builder /app/install/bin/dynamate /usr/local/bin/dynamate
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER dynamate
WORKDIR /home/dynamate

EXPOSE 7681

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
