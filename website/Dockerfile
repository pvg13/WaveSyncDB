# syntax=docker/dockerfile:1.6
#
# Build context: website/ directory (not the repo root).
#
# Stage 1 (builder): rust:bookworm + dioxus-cli + wasm32 target.
#   - Patches Cargo.toml to use wavesyncdb from crates.io (since the
#     workspace sibling crates aren't in this context).
#   - `dx build --platform web --release` produces a static-asset bundle.
#
# Stage 2 (runtime): nginx:alpine serving the static bundle.
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

# Patch Cargo.toml: replace workspace dependency with crates.io version.
# The website only uses the `web` + `dioxus` features which are published.
RUN sed -i 's|wavesyncdb = { workspace = true, features = \["web", "dioxus"\] }|wavesyncdb = { version = "0.6", features = ["web", "dioxus"] }|' Cargo.toml \
    && sed -i '/^\[workspace\]/d' Cargo.toml

# Build the WASM bundle. dx build writes output under target/dx/.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    dx build --platform web --release \
    && mkdir -p /out \
    && cp -r /src/target/dx/wavesync-website/release/web/public/. /out/

# -- Runtime: nginx serving the bundle --
FROM nginx:alpine

COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /out/ /usr/share/nginx/html/

EXPOSE 80
