# syntax=docker/dockerfile:1.6
#
# Two-stage build.
#
# Stage 1 (builder): rust:bookworm + cargo-chef + dioxus-cli + wasm32 target.
#   - cargo-chef plans the dependency graph from the workspace and cooks
#     deps once. Subsequent rebuilds reuse the cooked-deps layer if no
#     dependency changed, so iterating on `src/` or `content/` finishes
#     in ~30s on a warm cache instead of ~5 minutes from scratch.
#   - `dx build --platform web --release` produces a static-asset bundle
#     (no runtime server). Output lives under
#     /src/target/dx/wavesync-website/release/web/public/.
#
# Stage 2 (runtime): nginx:alpine. Tiny, no Rust runtime, gzip + correct
#   wasm MIME, SPA fallback to /index.html so the client-side router
#   handles /docs/:slug and /examples deep links.

# -- Plan stage (cargo-chef + dx CLI) --
FROM rust:1.92-bookworm AS chef
ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install cargo-chef --locked
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install dioxus-cli --version "0.7.0" --locked
RUN rustup target add wasm32-unknown-unknown
WORKDIR /src

# -- Recipe: capture dependency graph --
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# -- Build stage: cook deps once, then bundle the WASM --
FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json

# Cook deps for the website crate only. Cargo-chef compiles the
# dependency graph against the workspace; we scope to -p wavesync-website
# so we don't drag in unrelated workspace crates' build inputs.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo chef cook --release --recipe-path recipe.json -p wavesync-website --target wasm32-unknown-unknown

COPY . .

# `dx build` writes its bundle into a target subdir that disappears with
# the cache mount, so we copy it out to /out before the mount is released.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cd website && dx build --platform web --release \
    && mkdir -p /out \
    && cp -r /src/target/dx/wavesync-website/release/web/public/. /out/

# -- Runtime: nginx serving the bundle --
FROM nginx:alpine

COPY website/nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /out/ /usr/share/nginx/html/

EXPOSE 80

# nginx:alpine ENTRYPOINT/CMD already runs nginx in the foreground; no
# overrides needed.
