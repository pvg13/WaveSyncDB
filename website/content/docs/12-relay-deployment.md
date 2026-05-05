# Relay deployment

The `wavesync_relay` crate is a small libp2p server that does three things:

1. **Circuit relay v2** — bridges WAN peers behind NATs that can't connect directly.
2. **Rendezvous discovery** — peers register against a topic and discover each other.
3. **Push fan-out** — receives `notify_relay_topic` signals and triggers FCM/APNs to wake sleeping phones.

You only need a relay if your peers will be on different networks. LAN-only deployments don't need one — mDNS handles discovery automatically.

## Quick deploy with Docker Compose

A ready-to-deploy `docker-compose.yml` lives under `wavesync_relay/` in the repo. Drop a `.env` next to it:

```env
EXTERNAL_ADDRESS=/ip4/203.0.113.10/udp/4001/quic-v1,/ip4/203.0.113.10/tcp/4001
APNS_KEY_ID=ABC123XYZ4
APNS_TEAM_ID=DEF456UVW7
APNS_BUNDLE_ID=com.example.myapp
RUST_LOG=info
```

Place your secrets next to it under `secrets/`:

```
secrets/fcm.json      # FCM service-account JSON
secrets/apns.p8       # APNs signing key (PEM)
secrets/identity.b64  # base64-encoded libp2p keypair (optional, persists peer id)
```

Then:

```bash
docker compose up -d --build
```

## Deploying to Dokploy

The `docker-compose.yml` is already structured for Dokploy:

1. **Application type:** Docker Compose.
2. **Source:** this Git repo.
3. **Compose Path:** `wavesync_relay/docker-compose.yml`.
4. **Base Directory:** `wavesync_relay`.
5. **Advanced → Mounts:** add one **File Mount** per secret you use:

   | File path | Mount path | Content |
   |---|---|---|
   | `secrets/fcm.json` | `/run/secrets/fcm.json` | FCM service-account JSON |
   | `secrets/apns.p8` | `/run/secrets/apns.p8` | APNs `.p8` PEM |
   | `secrets/identity.b64` | `/run/secrets/identity.b64` | base64 identity |

   Dokploy stores these under the project's `files/` folder on the host — leave the compose `volumes:` block minimal so it doesn't conflict.

6. **Environment Variables:** set at minimum `EXTERNAL_ADDRESS`. Add APNs metadata if you use APNs.
7. Hit **Redeploy** (not Restart) to pick up new mounts.

## Environment variables

| Variable | Required | Notes |
|---|---|---|
| `EXTERNAL_ADDRESS` | yes | Public multiaddr clients dial. Comma-separated for multiple transports. |
| `IDENTITY_KEYPAIR` | no | Inline base64 keypair, or path to a file. Falls back to a generated key under `/data/identity.key`. |
| `FCM_CREDENTIALS` | no | Inline JSON or file path. Auto-discovered from `/run/secrets/fcm.json`. |
| `APNS_KEY_FILE` | no | Inline PEM or file path. Auto-discovered from `/run/secrets/apns.p8`. |
| `APNS_KEY_ID` / `APNS_TEAM_ID` / `APNS_BUNDLE_ID` | conditional | Required when APNs push is enabled. |
| `PUSH_DB` | no | Path to push token SQLite database. Defaults to `/data/push_tokens.db`. |
| `MAX_RESERVATIONS` | no | Default `1024`. Bump if you have many phones. |
| `MAX_RESERVATIONS_PER_PEER` | no | Default `32`. |
| `RESERVATION_DURATION_SECS` | no | Default `600`. |
| `RUST_LOG` | no | Standard log filter. `info` is recommended in production. |

## Pointing clients at it

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
    .with_passphrase("...")
    .with_relay_server("/ip4/203.0.113.10/udp/4001/quic-v1")
    .build()
    .await?;
```

For managed-relay mode (where the client also registers a push token):

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
    .with_passphrase("...")
    .managed_relay("/ip4/203.0.113.10/udp/4001/quic-v1", "your-relay-api-key")
    .build()
    .await?;
```

## Operating notes

- The relay is stateless except for `/data/identity.key` (peer id continuity) and `/data/push_tokens.db` (registered phones). Mount a volume on `/data` so reservations and tokens survive restarts.
- Health: check the logs for periodic `Reservation accepted` and `Push sent` lines. If you see `ReservationReqDenied { ResourceLimitExceeded }`, raise `MAX_RESERVATIONS_PER_PEER`.
- The relay can be public; without your shared passphrase, anonymous peers can't decrypt or sign anything they receive.
