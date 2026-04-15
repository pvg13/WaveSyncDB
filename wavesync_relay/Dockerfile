# -- Plan stage (cargo-chef) --
FROM rust:1.92-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /src

# -- Recipe: capture dependency graph --
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# -- Build stage: cook deps, then build --
FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p wavesync_relay

COPY . .
RUN cargo build --release -p wavesync_relay

# -- Runtime stage --
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/wavesync-relay /usr/local/bin/wavesync-relay

# /data stores persistent identity key and push token database
VOLUME /data

EXPOSE 4001/tcp 4001/udp

# Environment variables:
#   EXTERNAL_ADDRESS    - REQUIRED: public address to advertise to clients
#                         e.g., /ip4/77.37.125.212/tcp/4001
#                         Without this, clients get NoAddressesInReservation and
#                         WAN sync is broken. In Coolify, set this in Environment Variables.
#
#   IDENTITY_KEYPAIR    - base64-encoded protobuf keypair (overrides --identity-file)
#                         Generate with: wavesync-relay --generate-identity
#
# Push notification environment variables (all optional):
#   PUSH_DB             - path to push token SQLite DB (e.g., /data/push_tokens.db)
#
#   FCM_CREDENTIALS     - FCM service account JSON (inline JSON string or file path)
#
#   APNS_KEY_PEM        - APNs .p8 key (inline PEM string or file path)
#   APNS_KEY_ID         - APNs key ID from Apple Developer portal
#   APNS_TEAM_ID        - APNs team ID
#   APNS_BUNDLE_ID      - APNs app bundle ID (e.g., com.example.myapp)

ENTRYPOINT ["wavesync-relay"]
CMD ["--listen-addr", "/ip4/0.0.0.0/tcp/4001", "--identity-file", "/data/identity.key"]
