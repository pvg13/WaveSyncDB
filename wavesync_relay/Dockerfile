# syntax=docker/dockerfile:1.6
#
# BuildKit syntax version is required for `RUN --mount=type=cache`. These
# cache mounts persist the cargo registry, git checkouts, and the target
# directory across builds on the same builder (GHA's buildx-cache keeps
# them across CI runs). With no dependency changes, the cook + build
# stages are close to a no-op — most of the CI time ends up being the
# Docker layer push.

# -- Plan stage (cargo-chef) --
FROM rust:1.92-bookworm AS chef
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install cargo-chef --locked
WORKDIR /src

# -- Recipe: capture dependency graph --
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# -- Build stage: cook deps, then build --
FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo chef cook --release --recipe-path recipe.json -p wavesync_relay

COPY . .
# Final build reuses the cooked dependency cache; the binary is copied
# out of /src/target before the cache mount is released (otherwise it
# would disappear with the mount).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p wavesync_relay \
    && cp /src/target/release/wavesync-relay /usr/local/bin/wavesync-relay

# -- Runtime stage --
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/wavesync-relay /usr/local/bin/wavesync-relay

# /data stores persistent identity key and push token database
VOLUME /data

EXPOSE 4001/tcp 4001/udp

# Environment variables:
#   EXTERNAL_ADDRESS    - REQUIRED: public address to advertise to clients
#                         e.g., /ip4/77.37.125.212/tcp/4001
#                         Without this, clients get NoAddressesInReservation and
#                         WAN sync is broken.
#
#   IDENTITY_KEYPAIR    - base64-encoded protobuf keypair (inline or file path).
#                         Overrides --identity-file.
#                         Generate with: wavesync-relay --generate-identity
#
# All secret inputs (IDENTITY_KEYPAIR, FCM_CREDENTIALS, APNS_KEY_FILE) accept
# either the raw value inline OR a filesystem path to a mounted secret file.
#
# FCM_CREDENTIALS and APNS_KEY_FILE are also auto-discovered from the
# standard Docker secret mount points when not set explicitly:
#   /run/secrets/fcm.json   → FCM service account JSON
#   /run/secrets/apns.p8    → APNs signing key (still needs APNS_KEY_ID +
#                             APNS_TEAM_ID + APNS_BUNDLE_ID env vars)
# Mount the file at that path in your compose / Dokploy config and the
# relay picks it up without needing the env var.
#
# Push notification environment variables (all optional):
#   PUSH_DB             - path to push token SQLite DB (e.g., /data/push_tokens.db)
#   FCM_CREDENTIALS     - FCM service account JSON (inline or file path).
#                         Falls back to /run/secrets/fcm.json.
#   APNS_KEY_FILE       - APNs .p8 key (inline PEM or file path).
#                         Falls back to /run/secrets/apns.p8.
#   APNS_KEY_ID         - APNs key ID
#   APNS_TEAM_ID        - APNs team ID
#   APNS_BUNDLE_ID      - APNs app bundle ID (e.g., com.example.myapp)

# Default identity persistence path. The relay only writes here if the
# user hasn't supplied IDENTITY_KEYPAIR (which wins over IDENTITY_FILE).
# Overridable from docker-compose / Dokploy without needing a CMD override.
ENV IDENTITY_FILE=/data/identity.key

# Push token store location. Setting this enables the push notification
# subsystem at startup. Bake the default into the image because some
# orchestrators (notably Dokploy's compose renderer) ignore the
# `environment:` block of docker-compose.yml and only honour env vars
# set in the orchestrator's UI — meaning a hardcoded compose value like
# `PUSH_DB: /data/push_tokens.db` silently doesn't reach the container.
# An ENV here is image-baked and always present unless explicitly
# overridden, so push works out-of-the-box on any orchestrator.
ENV PUSH_DB=/data/push_tokens.db

ENTRYPOINT ["wavesync-relay"]
CMD []
