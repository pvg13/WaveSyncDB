# Relay monitoring on Dokploy — Prometheus + Grafana as separate services

The relay's compose service (`wavesync_relay/docker-compose.yml`) is
relay-only. Prometheus and Grafana run as **two separate Dokploy services**
using stock images, configured entirely in the Dokploy UI — because Dokploy's
Compose service type allows no UI-side edits to ports, mounts, or the compose
file itself.

How the pieces connect (all container-to-container, nothing published on the
host):

```
wavesync-relay:9464  ◄── scrape ──  prometheus:9090  ◄── datasource ──  grafana:3000  ◄── Dokploy Domain (HTTPS + login)
```

- The relay binds its metrics endpoint on `0.0.0.0:9464` inside the container
  (`METRICS_ADDR`), never publishes it, and pins `container_name:
  wavesync-relay` — so any container on the shared `dokploy-network` resolves
  `wavesync-relay:9464`.
- The files in this directory are the source of truth for the configuration
  you paste into Dokploy (and they back the local preview stack — see the
  end of this file).

---

## 1. Prometheus service

Dokploy → project → **Create Service → Application** (Docker image type).

| Setting | Value |
|---|---|
| Name | `prometheus` (exact — Grafana reaches it by this name) |
| Docker Image | `prom/prometheus:latest` |
| Domain | none. Never expose Prometheus. |
| Ports | none published |

**Mounts** (Application services allow real container paths):

- **File Mount** → Mount Path `/etc/prometheus/prometheus.yml`, content =
  [`prometheus/prometheus.yml`](prometheus/prometheus.yml) from this repo
  (scrapes `wavesync-relay:9464` every 15s).
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
  [`grafana/provisioning/datasources/prometheus.yml`](grafana/provisioning/datasources/prometheus.yml).
  This pins the datasource **uid to `prometheus`**, which the dashboard JSON
  references — creating the datasource by hand in the UI gives it a random
  uid and every imported panel breaks. Use the file.
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
   data, Grafana can't reach Prometheus (check both service names); if `0`,
   Prometheus runs but can't reach `wavesync-relay:9464` (is the relay
   deployed and on `dokploy-network`?).
2. Open the WaveSync Relay dashboard; generate some client traffic —
   Connected peers / Reservations / Bandwidth should move within ~30s.

## Notes

- **Nothing to configure on the relay side**: `METRICS_ADDR: 0.0.0.0:9464`
  and `container_name: wavesync-relay` are already in its compose file. The
  metrics port is intentionally unpublished — anything on `dokploy-network`
  can scrape it, the internet cannot.
- The Dockerfiles in this directory (`prometheus/Dockerfile`,
  `grafana/Dockerfile`) are used by the **local preview** stack, which runs
  the exact same configs with everything auto-provisioned:

  ```bash
  cd wavesync_relay
  docker compose -f docker-compose.yml -f docker-compose.local.yml up --build
  # Grafana: http://127.0.0.1:3000 (admin/admin) — Prometheus: http://127.0.0.1:9090
  ```
