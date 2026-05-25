# syntax=docker/dockerfile:1.6
#
# Build context: website/ directory.
#
# Patches Cargo.toml at build time to pull wavesyncdb from crates.io
# instead of the workspace path, so sibling crates aren't needed.
#
# Usage:
#   cd website && docker build -t wavesync-website .
#   docker compose up --build

FROM rust:1.92-bookworm AS builder
ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install dioxus-cli --version "0.7.0" --locked
RUN rustup target add wasm32-unknown-unknown
WORKDIR /src

COPY Cargo.toml Dioxus.toml ./
COPY src/ src/
COPY assets/ assets/
COPY content/ content/

# Replace workspace dep with crates.io version.
RUN sed -i 's|wavesyncdb = { workspace = true, features = \["web", "dioxus"\] }|wavesyncdb = { version = "0.6", features = ["web", "dioxus"] }|' Cargo.toml \
    && sed -i '/^\[workspace\]/d' Cargo.toml

# Dioxus.toml sets default_platform = "web"; dx 0.7 uses that automatically.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    dx build --release \
    && mkdir -p /out \
    && cp -r /src/target/dx/wavesync-website/release/web/public/. /out/

# -- Runtime: nginx serving the bundle --
FROM nginx:alpine

COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /out/ /usr/share/nginx/html/

EXPOSE 80
