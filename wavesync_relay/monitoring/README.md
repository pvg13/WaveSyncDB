# Relay monitoring on Dokploy — three Application services

On Dokploy, the relay, Prometheus, and Grafana all run as **Application**
services configured entirely in the Dokploy UI (env, file mounts to real
container paths, ports, domain). The Compose service type is deliberately not
used — it allows no UI-side edits to ports, mounts, or the compose file
(`docker-compose.yml` remains for plain docker-compose self-hosting and the
local preview stack).

Internal hostnames are **project-prefixed service names** (`<project>-<name>`
on the shared `dokploy-network`) — in a project called `almares`, services
named `relay` / `prometheus` / `grafana` are reachable as `almares-relay`,
`almares-prometheus`, `almares-grafana`. Substitute your own names below.

```
almares-relay:9464  ◄── scrape ──  almares-prometheus:9090  ◄── datasource ──  almares-grafana:3000  ◄── Dokploy Domain (HTTPS + login)
```

The files in this directory are the source of truth for the configuration
you paste into Dokploy (and they back the local preview stack — see the end
of this file).

---

## 0. Relay service

Dokploy → project → **Create Service → Application**.

| Setting | Value |
|---|---|
| Name | `relay` (→ internal hostname `<project>-relay`) |
| Provider | this Git repo, branch with the release you deploy |
| Build Type | Dockerfile |
| Docker File | `wavesync_relay/Dockerfile` |
| Docker Context Path | `.` (repo root — same as CI; the Dockerfile COPYs the whole workspace) |
| Domain | none (libp2p, not HTTP) |

**Ports** (Advanced → Ports): publish `4001` → `4001` **tcp** and `4001` →
`4001` **udp**. If a publish-mode choice is offered, pick **host** — ingress
(Swarm mesh) SNATs inbound connections, which makes libp2p `identify` report
a private IP back to clients and generates wasted AutoNAT dials (harmless but
noisy, since `EXTERNAL_ADDRESS` is authoritative anyway). Open 4001/tcp+udp
in the server firewall.

**Volume Mount**: named volume `relay-data` → `/data` (persists the identity
key = PeerId, `push_tokens.db`, `mailbox.db`). Never delete it — a new
identity breaks every client config pinning the old `/p2p/<PeerId>`.

**File Mounts** (only the push credentials you use — auto-discovered at
these exact paths, no env var needed):

| Mount Path | Content |
|---|---|
| `/run/secrets/fcm.json` | FCM service-account JSON |
| `/run/secrets/apns.p8` | APNs signing key PEM |

**Environment:**

```
EXTERNAL_ADDRESS=/dns4/relay.yourdomain.com/tcp/4001
METRICS_ADDR=0.0.0.0:9464
MAILBOX_DB=/data/mailbox.db
```

- `EXTERNAL_ADDRESS` — REQUIRED; without it clients get
  `NoAddressesInReservation`. Comma-separate to also advertise QUIC:
  `...,/dns4/relay.yourdomain.com/udp/4001/quic-v1`.
- `METRICS_ADDR=0.0.0.0:9464` — REQUIRED for monitoring: the binary's
  default is loopback-only (`127.0.0.1:9464`), unreachable from the
  Prometheus container. Do NOT publish 9464 in the Ports tab — container-
  to-container over `dokploy-network` needs no published port, and
  publishing it would expose metrics to the internet.
- `MAILBOX_DB` — enables the durable store-and-forward mailbox (off when
  unset). Deploy the relay with it before clients that use the mailbox.
- Plus APNs metadata if you use APNs: `APNS_KEY_ID`, `APNS_TEAM_ID`,
  `APNS_BUNDLE_ID` (and `APNS_SANDBOX=1` for dev builds).
- Already baked into the image (do not set): `PUSH_DB=/data/push_tokens.db`,
  `IDENTITY_FILE=/data/identity.key`.

Deploy, then check logs for: the PeerId, listen addresses,
`Metrics endpoint on http://0.0.0.0:9464/metrics`, and (if enabled)
`Mailbox enabled (db: /data/mailbox.db, ...)`.

---

## 1. Prometheus service

Dokploy → project → **Create Service → Application** (Docker image type).

| Setting | Value |
|---|---|
| Name | `prometheus` |
| Docker Image | `prom/prometheus:latest` |
| Domain | none. Never expose Prometheus. |
| Ports | none published |

**Mounts** — File Mount → Mount Path `/etc/prometheus/prometheus.yml`,
content = [`prometheus/prometheus.yml`](prometheus/prometheus.yml) **with one
edit**: change the scrape target `wavesync-relay:9464` to your relay
service's project-prefixed internal name, e.g. `almares-relay:9464` (the repo
file's un-prefixed name is for the local preview stack).

- **Volume Mount** → name `prometheus-data`, Mount Path `/prometheus`
  (metric history survives redeploys).

**Retention** (optional): stock default is 15d. If the service's Run/Command
field is editable, set the full command to:

```
--config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus --storage.tsdb.retention.time=60d
```

(These are the image's default args plus the retention flag — the first two
must be repeated because overriding the command replaces them.)

Deploy, then check the service logs for `Server is ready to receive web
requests`.

## 2. Grafana service

Dokploy → project → **Create Service → Application**.

| Setting | Value |
|---|---|
| Name | `grafana` |
| Docker Image | `grafana/grafana-oss:latest` |
| Domain | `grafana.yourdomain.com` → container port `3000`, HTTPS on |

**Environment** (env-var equivalents of [`grafana/grafana.ini`](grafana/grafana.ini)):

```
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=<your password>
GF_USERS_ALLOW_SIGN_UP=false
GF_AUTH_ANONYMOUS_ENABLED=false
GF_METRICS_ENABLED=false
GF_ANALYTICS_REPORTING_ENABLED=false
GF_ANALYTICS_CHECK_FOR_UPDATES=false
```

`GF_METRICS_ENABLED=false` matters: Grafana's own `/metrics` is otherwise
served **unauthenticated** through the public domain.

`GF_SECURITY_ADMIN_PASSWORD` applies on **first boot only** — once the
`grafana-data` volume holds a database, changing the env var is a silent
no-op; rotate the password in Grafana's UI instead (or `grafana-cli admin
reset-admin-password`). After the first deploy, verify your password actually
works at the domain; if `admin/admin` is still accepted, set the password in
the UI — it persists in the volume.

**Mounts:**

- **File Mount** → Mount Path
  `/etc/grafana/provisioning/datasources/prometheus.yml`, content =
  [`grafana/provisioning/datasources/prometheus.yml`](grafana/provisioning/datasources/prometheus.yml)
  **with one edit**: change `url: http://prometheus:9090` to your Prometheus
  service's project-prefixed internal name, e.g.
  `url: http://almares-prometheus:9090` (the repo file's un-prefixed name is
  for the local preview stack). Keep `uid: prometheus` EXACTLY as-is — the
  dashboard JSON references that uid; creating the datasource by hand in the
  UI gives it a random uid and every imported panel breaks. Use the file.
- **Volume Mount** → name `grafana-data`, Mount Path `/var/lib/grafana`.

Deploy, log in at the domain.

## 3. Import the dashboard

Grafana → **Dashboards → New → Import** → paste the contents of
[`grafana/dashboards/wavesync-relay.json`](grafana/dashboards/wavesync-relay.json)
→ Import. The panels bind to the provisioned datasource automatically (uid
`prometheus`).

The import is stored in `grafana-data`, so it survives redeploys. When the
dashboard JSON changes in the repo, re-import (Import → paste → it offers to
overwrite the existing dashboard with the same uid). UI edits you want to
keep should be saved as a copy first — a re-import overwrites the
`wavesync-relay` uid.

## 4. Verify end-to-end

1. Grafana → Explore → query `up{job="wavesync-relay"}` → value `1` means
   Prometheus is up AND scraping the relay successfully. If it returns no
   data, Grafana can't reach Prometheus (check the datasource URL against
   the real service name); if `0`, Prometheus runs but can't reach the relay
   (wrong scrape target name, relay not deployed, or METRICS_ADDR not set to
   0.0.0.0:9464).
2. Open the WaveSync Relay dashboard; generate some client traffic —
   Connected peers / Reservations / Bandwidth should move within ~30s.

## Notes

- The metrics port 9464 is intentionally unpublished — anything on
  `dokploy-network` can scrape it, the internet cannot.
- The Dockerfiles in this directory (`prometheus/Dockerfile`,
  `grafana/Dockerfile`) are used by the **local preview** stack, which runs
  the exact same configs with everything auto-provisioned:

  ```bash
  cd wavesync_relay
  docker compose -f docker-compose.yml -f docker-compose.local.yml up --build
  # Grafana: http://127.0.0.1:3000 (admin/admin) — Prometheus: http://127.0.0.1:9090
  ```
