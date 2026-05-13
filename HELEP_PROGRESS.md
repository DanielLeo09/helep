# HELEP Capstone — Progress Log

This file documents every step we take together, what was done, where to find it, and why.
Updated as we complete each part.

---

## Project Parts Overview

| Part | Description | Status |
|------|-------------|--------|
| A | Code completions inside the starter | ✅ Done |
| B | Dockerfiles (one per service) | ⏳ Next |
| C | Kubernetes manifests | 🔲 Pending |
| D | Kafka on K8s via Strimzi | 🔲 Pending |
| E | Prometheus + Grafana monitoring | 🔲 Pending |
| F | CI/CD pipeline | 🔲 Pending |
| G | L4 Design Process Document | ✅ Written (PDF + diagrams pending) |
| H | L3 Patterns-in-Code Document | ✅ Written (PDF pending) |

---

## Part A — Code Completions

**Goal:** Complete the stub code the lecturer left for the student, and add 2 new patterns.

### A.1 — Circuit Breaker (required by spec)

**What it is:** A fault-tolerance pattern that stops calls to a failing dependency.
It has 3 states:
- **CLOSED** → normal operation. Failures are counted.
- **OPEN** → too many failures. All calls are blocked immediately (fast fail).
- **HALF_OPEN** → after a timeout, one probe call is allowed. If it succeeds, go back to CLOSED. If it fails, go back to OPEN.

**What was broken:** The `allow()` method in `CircuitBreaker` just returned `True` always. The state machine was never implemented.

**What we did:** Completed `allow()`, `record_success()`, and `record_failure()` with the full state machine using `time.monotonic()` for the OPEN timeout.

**Files changed (same change in all 5 services):**

| File | Key lines |
|------|-----------|
| `services/user-service/app/events.py` | Lines 57–80 — CircuitBreaker class |
| `services/sos-service/app/events.py` | Lines 57–80 — CircuitBreaker class |
| `services/dispatch-service/app/events.py` | Lines 57–80 — CircuitBreaker class |
| `services/notification-service/app/events.py` | Lines 57–80 — CircuitBreaker class |
| `services/analytics-service/app/events.py` | Lines 57–80 — CircuitBreaker class |

**State machine logic (plain English):**
1. `allow()` is called before every Kafka publish.
2. If `_state == CLOSED` → return True (normal).
3. If `_state == OPEN` → check if `reset_after_s` seconds have passed since `opened_at`.
   - If yes → switch to HALF_OPEN, return True (one probe).
   - If no → return False (blocked).
4. If `_state == HALF_OPEN` → return True (the one probe call).
5. `record_success()` → reset fails to 0, go back to CLOSED.
6. `record_failure()` → increment fails. If HALF_OPEN OR fails ≥ threshold → go to OPEN, record timestamp.

---

### A.2 — Third Strategy: RoundRobinMatcher (required by spec)

**What it is:** The Strategy pattern lets you swap algorithms at runtime. The starter had 2 strategies:
- `NearestMatcher` — picks the geographically closest responder
- `CredibilityWeightedMatcher` — balances distance and trustworthiness score

**What we added:** `RoundRobinMatcher` — ignores location entirely, cycles through responders in sequence. Selected by setting env var `MATCHER=roundrobin`.

**File changed:** `services/dispatch-service/app/matching.py`

**Why round-robin?** Useful when all responders are roughly equidistant (e.g. a small zone) and you want to distribute workload evenly rather than always overloading the nearest unit.

---

### A.3 — New Pattern B.1: Idempotency Key (notification-service)

**What it is:** When Kafka re-delivers a message (at-least-once guarantee), the handler runs again. Without protection, this sends duplicate SMS. An Idempotency Key ensures "process this event at most once per unique key".

**What we did:**
- Added `incident_id TEXT` column to the `notifications` table in `notification-service/app/db.py`
- Added `UNIQUE(incident_id, template)` constraint on that table
- Changed `record()` to use `INSERT OR IGNORE` — silently drops duplicate delivery attempts
- Updated `notification-service/app/main.py` to pass `incident_id` to `record()`

**Files changed:**

| File | Change |
|------|--------|
| `services/notification-service/app/db.py` | Added `incident_id` column + UNIQUE constraint + INSERT OR IGNORE |
| `services/notification-service/app/main.py` | Pass `incident_id` to `record()` call |

**Why this matters:** Kafka guarantees at-least-once delivery. Without this, a restarted notification-service pod would re-send SMS for every unacknowledged message since its last crash.

---

### A.4 — New Pattern B.2: Retry with Exponential Backoff (all services)

**What it is:** When a Kafka event handler fails (e.g. transient DB error), instead of immediately giving up (leaving the message uncommitted), retry up to 3 times with increasing wait times: 1s, 2s, then give up. This reduces noise from transient failures without infinite retries.

**What we did:** Updated `consume()` in all 5 `events.py` files — replaced the bare `try/except pass` with a retry loop.

**Files changed (same change in all 5 services):**
- `services/user-service/app/events.py` — `consume()` function
- `services/sos-service/app/events.py` — `consume()` function
- `services/dispatch-service/app/events.py` — `consume()` function
- `services/notification-service/app/events.py` — `consume()` function
- `services/analytics-service/app/events.py` — `consume()` function

**Retry schedule:** attempt 0 → instant, attempt 1 → wait 1s, attempt 2 (final) → wait 2s, then leave uncommitted (Kafka will redeliver on next pod start).

---

## Part B — Dockerfiles

**Status: ✅ Done**

Each service has a `Dockerfile` that packages it into a Docker image. All 5 are identical in structure — only the port number differs.

### What each line does

```dockerfile
FROM python:3.12-slim          # Start from official Python 3.12 (slim = smaller image)
WORKDIR /app                   # All commands run inside /app in the container
COPY requirements.txt .        # Copy dependencies list first (Docker cache trick)
RUN pip install --no-cache-dir -r requirements.txt  # Install dependencies
COPY . .                       # Copy the rest of the service code
EXPOSE <PORT>                  # Document which port this container uses
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "<PORT>"]  # Start server
```

**Why `COPY requirements.txt` before `COPY . .`?**
Docker builds in layers. If you copy requirements first and install them, that layer is cached. Next time you rebuild after only changing Python code, Docker skips reinstalling packages — much faster.

**Why `--host 0.0.0.0`?**
Inside a container, `localhost` means the container itself. `0.0.0.0` means "listen on all network interfaces" so traffic from outside the container (Kubernetes, other services) can reach it.

### Files created

| File | Port |
|------|------|
| `services/user-service/Dockerfile` | 8001 |
| `services/sos-service/Dockerfile` | 8002 |
| `services/dispatch-service/Dockerfile` | 8003 |
| `services/notification-service/Dockerfile` | 8004 |
| `services/analytics-service/Dockerfile` | 8005 |

---

## Part C — Kubernetes Manifests

**Status: ✅ Done**

### What Kubernetes manifests are

A manifest is a YAML file that tells Kubernetes "here is something I want you to create and manage." You write the file, run `kubectl apply -f <file>`, and Kubernetes does the work.

### Folder structure created

```
k8s/
├── namespace.yaml                          ← creates the "helep" namespace
├── ingress.yaml                            ← routes external HTTP traffic to all 5 services
├── user-service/
│   ├── configmap.yaml                      ← non-secret env vars (Kafka address, port, DB path)
│   ├── secret.yaml                         ← JWT_SECRET (encrypted at rest by K8s)
│   ├── pvc.yaml                            ← 1Gi persistent disk for SQLite
│   ├── deployment.yaml                     ← runs the container, mounts disk, health checks
│   ├── service.yaml                        ← internal hostname "user-service:8001"
│   ├── hpa.yaml                            ← autoscale 1–5 pods at 60% CPU
│   └── networkpolicy.yaml                  ← firewall: allow ingress + helep pods only
├── sos-service/         (same 7 files, port 8002)
├── dispatch-service/    (same 7 files, port 8003) ← also has MATCHER=nearest in configmap
├── notification-service/(same 7 files, port 8004)
└── analytics-service/   (same 7 files, port 8005)
```

### What each file type does

| File | Purpose |
|------|---------|
| **namespace.yaml** | Groups all HELEP resources together in a namespace called `helep` |
| **configmap.yaml** | Stores non-sensitive env vars: `KAFKA_BOOTSTRAP`, `SERVICE_PORT`, `DB_PATH`, `MATCHER` (dispatch only) |
| **secret.yaml** | Stores `JWT_SECRET`. Written in plain text (`stringData`), K8s encodes and encrypts it automatically |
| **pvc.yaml** | Requests a 1GB disk (`ReadWriteOnce`). Mounted at `/data` so the SQLite file survives pod restarts |
| **deployment.yaml** | Main file: run 1 replica of the container, load env vars from ConfigMap + Secret, mount PVC at `/data`, use `/healthz` and `/readyz` for health checks, set CPU/memory limits |
| **service.yaml** | Gives the deployment a stable internal hostname so other services and the Ingress can reach it by name |
| **hpa.yaml** | Horizontal Pod Autoscaler: if CPU > 60%, add more pods automatically (min 1, max 5) |
| **networkpolicy.yaml** | Firewall: default-deny all traffic, then explicitly allow inbound from Ingress controller + helep namespace pods, and outbound to Kafka + DNS only |
| **ingress.yaml** | Single front door: routes external HTTP by URL path (`/api/users/*` → user-service, `/api/sos/*` → sos-service, etc.) |

### Key design decisions

- **replicas: 1** in all deployments — SQLite uses `ReadWriteOnce` (one pod per disk). More replicas would require multiple pods competing for the same file. This is noted as a known trade-off (design doc Part G).
- **dispatch-service configmap** has an extra `MATCHER: "nearest"` env var — the only service that uses the Strategy pattern switching.
- **JWT_SECRET value** (`helep-jwt-secret-change-in-production`) is a placeholder — must be changed to a strong random string before a real deployment.
- **Image names** (e.g. `helep/user-service:latest`) are placeholders — the CI/CD pipeline (Part F) will replace these with the actual registry path.

---

## Part D — Strimzi Kafka on Kubernetes

**Status: ✅ Done**

### What Strimzi is

Strimzi is a Kubernetes Operator — a program that runs inside Kubernetes and knows how to create and manage Apache Kafka clusters. Instead of manually configuring Kafka pods, you write a YAML file describing what you want and Strimzi builds it for you.

### Folder created

```
k8s/kafka/
├── kafka-cluster.yaml             ← defines the Kafka cluster itself
├── topic-user-registered.yaml     ← user.registered topic
├── topic-sos-triggered.yaml       ← sos.triggered topic
├── topic-sos-cancelled.yaml       ← sos.cancelled topic
├── topic-responder-assigned.yaml  ← responder.assigned topic
├── topic-responder-confirmed.yaml ← responder.confirmed topic
├── topic-safety-zone-entered.yaml ← safety.zone.entered topic
└── topic-notification-sent.yaml   ← notification.sent topic
```

### What kafka-cluster.yaml does

Creates a Kafka cluster named `kafka-cluster` inside the `helep` namespace using Strimzi's `Kafka` custom resource. Configuration choices:

| Setting | Value | Why |
|---------|-------|-----|
| `replicas: 1` | 1 broker | Single node is enough for this project |
| `zookeeper.replicas: 1` | 1 ZooKeeper | Coordinates the broker |
| `storage: ephemeral` | In-memory | Simpler for a student project; real production would use `persistent-claim` |
| `tls: false` | No encryption | Keeps internal cluster comms simple |
| `replication.factor: 1` | No data replication | Only 1 broker so replication > 1 is impossible |
| `entityOperator.topicOperator` | Enabled | This makes Strimzi read our KafkaTopic files and create topics automatically |

Once this is applied, other services can reach Kafka at the address:
`kafka-cluster-kafka-bootstrap:9092` — which is exactly what every service's ConfigMap already has.

### What each KafkaTopic file does

Each file creates one Kafka topic. All 7 topics use the same settings:

| Setting | Value | Why |
|---------|-------|-----|
| `partitions: 3` | 3 partitions | Allows up to 3 consumer pods to process events in parallel |
| `replicas: 1` | No replication | Only 1 broker available |
| `retention.ms: 604800000` | 7-day retention | Events kept for 7 days before automatic deletion |

**Important naming:** Kubernetes resource names cannot contain dots, so `metadata.name` uses hyphens (e.g. `sos-triggered`) while `spec.topicName` uses the real dot notation (e.g. `sos.triggered`) that matches what the Python code produces.

### To install Strimzi on a real cluster (run once)

```bash
kubectl create -f https://strimzi.io/install/latest?namespace=helep -n helep
```

Then apply the Kafka files:
```bash
kubectl apply -f k8s/kafka/kafka-cluster.yaml
kubectl apply -f k8s/kafka/
```

---

## Part E — Prometheus + Grafana Monitoring

**Status: ✅ Done**

### What already existed (no changes needed)

Every service already had monitoring built in from the starter code:
- `make_asgi_app()` mounts a `/metrics` endpoint on every service
- `Counter(...)` objects track events (e.g. `helep_notifications_sent_total`, `helep_analytics_events_total`)

Part E just adds the tools that read those endpoints.

### Folder created

```
k8s/monitoring/
├── prometheus-configmap.yaml          ← scrape config (which services to monitor)
├── prometheus-pvc.yaml                ← 2Gi disk for metrics storage
├── prometheus-deployment.yaml         ← runs Prometheus container
├── prometheus-service.yaml            ← internal ClusterIP on port 9090
├── grafana-datasource-configmap.yaml  ← auto-connects Grafana to Prometheus
├── grafana-deployment.yaml            ← runs Grafana container
└── grafana-service.yaml               ← NodePort on 30300 (open in browser)
```

### How the two tools connect

```
[user-service:8001/metrics] ──┐
[sos-service:8002/metrics]  ──┤
[dispatch-service:8003/metrics] ─► Prometheus (port 9090) ─► Grafana (port 3000)
[notification-service:8004/metrics] ─┘                         ↑
[analytics-service:8005/metrics] ───┘           auto-wired by grafana-datasource-configmap
```

### What each file does

| File | Key detail |
|------|-----------|
| **prometheus-configmap.yaml** | Lists all 5 services as scrape targets. Prometheus visits `/metrics` on each one every 15 seconds |
| **prometheus-pvc.yaml** | 2Gi disk — stores 7 days of metrics history (`--storage.tsdb.retention.time=7d`) |
| **prometheus-deployment.yaml** | Runs `prom/prometheus:latest`, mounts the config file and the data disk |
| **prometheus-service.yaml** | ClusterIP — makes Prometheus reachable at `http://prometheus:9090` inside the cluster |
| **grafana-datasource-configmap.yaml** | Grafana provisioning file — automatically adds Prometheus as the default data source so you do not have to configure it manually in the UI |
| **grafana-deployment.yaml** | Runs `grafana/grafana:latest`. Default login: `admin` / `helep-admin` |
| **grafana-service.yaml** | NodePort on 30300 — open `http://<your-node-ip>:30300` in a browser to see dashboards |

### Commands to apply monitoring (run after cluster is up)

```bash
kubectl apply -f k8s/monitoring/
```

Then open Grafana in your browser at `http://<cluster-ip>:30300` and log in with `admin` / `helep-admin`. Prometheus is already wired as the data source — go to Explore and query any metric like `helep_notifications_sent_total`.

---

## Part F — CI/CD Pipeline

**Status: ✅ Done**

### File created

```
.github/workflows/ci.yml
```

### What CI/CD means

- **CI (Continuous Integration)** — every time you push code to GitHub, it is automatically built and tested
- **CD (Continuous Delivery)** — after a successful build, the new image is automatically pushed to Docker Hub ready for deployment

### What the pipeline does (step by step)

| Step | Action |
|------|--------|
| Trigger | Runs automatically on every push to the `main` branch |
| Checkout | Downloads your code onto the GitHub Actions runner machine |
| Docker login | Logs into Docker Hub using your stored secrets |
| Build & push ×5 | Builds one Docker image per service using the Dockerfile in each service folder, then pushes it to Docker Hub |

### Image names produced

Each image is pushed to Docker Hub as:
`<your-dockerhub-username>/helep-<service-name>:latest`

For example: `yourusername/helep-user-service:latest`

### Two GitHub Secrets you must add (do this when you create the repo)

Go to your GitHub repo → Settings → Secrets and variables → Actions → New repository secret

| Secret name | What to put |
|-------------|------------|
| `DOCKERHUB_USERNAME` | Your Docker Hub username |
| `DOCKERHUB_TOKEN` | A Docker Hub access token (create at hub.docker.com → Account Settings → Security → New Access Token) |

### When to run

The pipeline runs automatically the moment you push code to the `main` branch on GitHub. No manual command needed.

### Full deployment order (for when you are ready to deploy)

1. Create GitHub repo and push all code
2. Add the two secrets above
3. Push any change to trigger the pipeline → images appear on Docker Hub
4. Run kubectl commands to apply K8s manifests (from Parts C, D, E)

---

## Part G — L4 Design Process Document

**Status: ✅ Written — PDF export + diagrams pending**

File: `design-process-template.md`
All 9 sections filled in. Still needed before export:
- 3 diagrams (system overview, saga flow, K8s deployment)
- Export to PDF: right-click in VS Code → Markdown PDF: Export (pdf) → rename output to `design.pdf`

---

## Part H — L3 Patterns-in-Code Document

**Status: ✅ Written — PDF export pending**

File: `patterns-template.md`
All sections filled in (A.1–A.6, B.1–B.2, Part C).
- Export to PDF: right-click in VS Code → Markdown PDF: Export (pdf) → rename output to `patterns.pdf`
