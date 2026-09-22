# Docker Monitored Web Server Lab

Dockerized stack that monitors an Nginx server and sends email alerts on downtime/recovery, with Redis-backed dedup/locking, RabbitMQ retry+DLQ, and Prometheus/Grafana monitoring.

```
                    ┌─────────────┐
        HTTP        │             │
   ┌───────────────▶│    Nginx    │◀──── health polled every 5s
   │                │  (port 8080)│
   │                └─────────────┘
   │                        ▲
   │                        │ GET /health
   │                ┌───────┴────────┐
   │                │    Watcher     │  consecutive failure/recovery
   │                │ (health poller)│  thresholds + Redis lock/cooldown
   │                └───────┬────────┘
   │                        │ publish AlertEvent
   │                        ▼
   │              ┌───────────────────┐
   │              │      RabbitMQ      │  direct exchange + retry/DLX topology
   │              │ (ports 5672/15672) │
   │              └─────────┬──────────┘
   │                        │ consume (prefetch=1)
   │                        ▼
   │                ┌───────────────┐        ┌──────────┐
   │                │     Mailer    │───────▶│  SMTP    │
   │                │  (port 3000)  │        │ Provider │
   │                └───────┬───────┘        └──────────┘
   │                        │ /metrics
   │                        ▼
   │                ┌───────────────┐        ┌───────────┐
   │                │  Prometheus   │───────▶│  Grafana  │
   │                │  (port 9090)  │        │(port 3001)│
   │                └───────────────┘        └───────────┘
   │
   └── Redis (state, idempotency keys, distributed locks, cooldowns)
```

## Services

| Service      | Role                                             | Port(s)      |
|--------------|---------------------------------------------------|--------------|
| `nginx`      | Monitored web server (`/`, `/health`)             | 8080         |
| `watcher`    | Polls Nginx health, raises DOWN/RECOVERED alerts  | –            |
| `rabbitmq`   | Broker with retry/DLQ topology                    | 5672, 15672  |
| `mailer`     | Consumes alerts, sends email, exposes metrics     | 3000         |
| `redis`      | Locks, cooldowns, idempotency state               | –            |
| `prometheus` | Scrapes mailer metrics, evaluates alert rules     | 9090         |
| `grafana`    | Dashboards                                        | 3001         |

## Setup

```bash
# SMTP password (Docker secret, gitignored)
echo -n "your-smtp-password" > secrets/smtp_password.txt

# Mailer env
cp mailer/.env.example mailer/.env
# fill in SMTP_HOST, SMTP_USER, ALERT_EMAIL (leave SMTP_PASSWORD empty)
```

## Run

```bash
docker compose up --build
```

Test it: `docker compose stop nginx` → watcher fires a DOWN alert after 3 failed checks → `docker compose start nginx` → RECOVERED alert after 2 successful checks.

## Notes

- Alert events are idempotent (unique `eventId`, Redis dedup) — safe on retries/duplicates.
- Failed mailer processing retries via RabbitMQ (5s delay, 3 attempts) then lands in `alert.dlq`.
- `watcher`/`mailer` run read-only, non-root, with all Linux capabilities dropped.
- Alert rules: `prometheus/alerts.yml` (high failure rate, high retry rate, high P95 latency).
