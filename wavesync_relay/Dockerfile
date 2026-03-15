# -- Build stage --
FROM rust:1.92-bookworm AS builder

WORKDIR /src

# Copy workspace manifests and lock file first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY wavesync_relay/Cargo.toml wavesync_relay/Cargo.toml
COPY wavesyncdb/Cargo.toml wavesyncdb/Cargo.toml
COPY wavesyncdb_derive/Cargo.toml wavesyncdb_derive/Cargo.toml

# Stub out workspace members so cargo can resolve deps
RUN mkdir -p wavesync_relay/src wavesyncdb/src wavesyncdb_derive/src \
    && echo "fn main() {}" > wavesync_relay/src/main.rs \
    && echo "" > wavesyncdb/src/lib.rs \
    && echo "" > wavesyncdb_derive/src/lib.rs \
    && mkdir -p examples/dioxus_app/src examples/dioxus_dynamic/src examples/p2p/src examples/wan/src \
    && printf '[package]\nname = "dioxus_app"\nversion = "0.1.0"\nedition = "2024"\n' > examples/dioxus_app/Cargo.toml \
    && printf '[package]\nname = "dioxus_dynamic"\nversion = "0.1.0"\nedition = "2024"\n' > examples/dioxus_dynamic/Cargo.toml \
    && printf '[package]\nname = "example-p2p"\nversion = "0.1.0"\nedition = "2024"\n' > examples/p2p/Cargo.toml \
    && printf '[package]\nname = "example-wan"\nversion = "0.1.0"\nedition = "2024"\n' > examples/wan/Cargo.toml \
    && echo "fn main() {}" > examples/dioxus_app/src/main.rs \
    && echo "fn main() {}" > examples/dioxus_dynamic/src/main.rs \
    && echo "fn main() {}" > examples/p2p/src/main.rs \
    && echo "fn main() {}" > examples/wan/src/main.rs

# Pre-build dependencies (cached unless Cargo.toml/lock change)
RUN cargo build --release -p wavesync_relay 2>/dev/null || true

# Copy real source code
COPY wavesync_relay/src wavesync_relay/src
COPY wavesyncdb/src wavesyncdb/src
COPY wavesyncdb_derive/src wavesyncdb_derive/src

# Touch main.rs to invalidate the stub build
RUN touch wavesync_relay/src/main.rs

RUN cargo build --release -p wavesync_relay

# -- Runtime stage --
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/wavesync-relay /usr/local/bin/wavesync-relay

# /data stores persistent identity key and push token database
VOLUME /data

EXPOSE 4001/tcp 4001/udp

# Push notification environment variables (optional):
#   PUSH_DB             - path to push token SQLite DB (e.g., /data/push_tokens.db)
#   FCM_CREDENTIALS     - path to FCM service account JSON file
#   APNS_KEY_FILE       - path to APNs .p8 key file
#   APNS_KEY_ID         - APNs key ID
#   APNS_TEAM_ID        - APNs team ID
#   APNS_BUNDLE_ID      - APNs app bundle ID (e.g., com.example.myapp)

ENTRYPOINT ["wavesync-relay"]
CMD ["--listen-addr", "/ip4/0.0.0.0/tcp/4001", "--identity-file", "/data/identity.key"]
