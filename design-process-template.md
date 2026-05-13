# L4 Design Process Document — HELEP (Template)


## 1. Project Specification

HELEP (Help Emergency Location Platform) is a real-time emergency response. The system is targeting Cameroon's urban areas. When a citizen is in danger, they trigger an SOS from a mobile device, providing their GPS coordinates. The system automatically identifies the nearest available responder, dispatches them and sends SMS or push notifications to all relevant parties.

The system serves four types of users:
- **Citizens**: trigger SOS alerts and receive confirmation
- **Responders**: receive dispatch orders and confirm attendance  
- **Police / Admin**: monitor incidents through a live statistics dashboard
- **System Admins**: manage users, responders, and system configuration

The business value is reducing emergency response time in regions where manual dispatch is slow and unreliable. By automating the full chain from alert to dispatch to notification, HELEP targets sub-1-second notification delivery even under high load.


## 2. Requirements Analysis

### 2.1 Functional requirements (from SRS §2)

| # | Requirement | Source |
|---|-------------|--------|
| F1 | Users can register with phone number, password, and role (citizen/responder/police) | SRS §2 — User Management |
| F2 | Authenticated citizens can trigger an SOS with GPS coordinates in online or offline mode | SRS §2 — Emergency Component |
| F3 | Citizens can cancel an active SOS incident | SRS §2 — Emergency Component |
| F4 | System assigns the nearest available responder to an incident automatically | SRS §2 — Incident Response & Localization |
| F5 | Only one responder is assigned per incident at any moment | SRS §2 — Constraint |
| F6 | Responders and citizens receive SMS or push notifications on dispatch | SRS §2 — Alert Management |
| F7 | Responders can confirm they have received a dispatch order | SRS §2 — Incident Response |
| F8 | Police can view live incident statistics and zone heatmaps | SRS §2 — Analytics & Statistics |



### 2.2 Non-functional requirements

| NFR | Measurable Acceptance Criterion |
|-----|--------------------------------|
| Availability | Services restart automatically on failure; 99.9% uptime enforced by K8s liveness probes and HPA |
| Usability | All REST endpoints return a response within 200ms under normal load |
| Confidentiality | Every protected endpoint validates a JWT signed with a secret stored in a K8s Secret object; no plain-text credentials in code |
| Integrity | Only one responder assigned per incident, enforced by atomic SQL UPDATE WHERE busy=0 and a UNIQUE PRIMARY KEY on the assignment row |
| Reliability | 99% of SOS notifications delivered within 1 second under 100 simultaneous requests |
| Scalability | HPA scales any service from 1 to 5 pods automatically when CPU exceeds 60%, with no manual intervention |
| Compatibility | All APIs use REST with JSON payloads, compatible with any HTTP client including mobile and web browsers |


### 2.3 Constraints (SRS §4)

**Constraint 1: One responder per incident at any time**
The SRS forbids dispatching two responders to the same incident simultaneously.
Architectural risk: with multiple dispatch-service pods consuming Kafka events in parallel, two pods could race to claim the same responder for the same incident.
Mitigation: the dispatch-service uses an atomic SQL statement (UPDATE responders SET busy=1 WHERE id=? AND busy=0) and a UNIQUE PRIMARY KEY on the assignments table keyed by incident_id. Only one pod wins the race; the other gets a constraint violation and does not assign.

**Constraint 2: Alert must reach the responder within 1 second of SOS trigger**
The SRS requires sub-second notification delivery.
Architectural risk: any synchronous HTTP call between services (e.g. SOS calling dispatch directly) would add network latency and coupling, making the 1-second target unreliable.
Mitigation: the system uses Apache Kafka for all inter-service communication. Events are small JSON payloads delivered in-memory to local consumer groups, keeping the full trigger → assign → notify chain well within 1 second on a local broker.

## 3. Architectural Drivers & ASRs (Lecture 1 material)

The three most Architecturally Significant Requirements (ASRs) are:

### ASR-1: Reliability — Sub-second notification delivery
- **Quality attribute:** Reliability / Performance
- **Requirement:** An SOS trigger must result in a responder notification within 1 second (SRS §4 Constraint2)
- **Why it is architecturally significant:** This single constraint eliminates any architecture that relies on synchronous HTTP calls between services. It forces an event-driven design where the entire trigger→assign→notify chain runs asynchronously through a message broker. Every inter-service communication choice in this system is driven by this ASR.

### ASR-2: Integrity — No double dispatch
- **Quality attribute:** Integrity / Correctness
- **Requirement:** Only one responder must be assigned to one incident at any moment (SRS §4 Constraint 1)
- **Why it is architecturally significant:** With multiple service replicas consuming the same Kafka events, race conditions are inevitable without a concurrency control mechanism. This ASR forces the use of atomic database operations and unique constraints at the persistence layer architectural choices that affect every deployment decision.

### ASR-3: Scalability — Handle emergency spikes
- **Quality attribute:** Scalability
- **Requirement:** System must handle sudden spikes in SOS volume without manual intervention (SRS §3 — Scalability NFR)
- **Why it is architecturally significant:** This rules out any stateful shared-memory architecture. Services must be stateless (state lives in the database, not in the pod) so Kubernetes can add or remove replicas freely. It also drives the decision to use Kafka consumer groups adding a pod automatically shares the event load via partition assignment.


## 4. Component Identification (Lecture 4 step)

### 4.1 SRS-listed components

1. User Management
2. Emergency Component
3. Incident Report & Response
4. Localization
5. Alert Management
6. Alert Delivery
7. Feedback & Review
8. Analytics & Statistics

### 4.2 Your service decomposition
| SRS Component | Service | Justification |
|---------------|---------|---------------|
| User Management | user-service (port 8001) | Standalone — owns identity, JWT, and credibility. No reason to merge with other concerns. |
| Emergency Component | sos-service (port 8002) | Standalone — owns the SOS lifecycle (trigger, cancel, media stub). Isolated so it can scale independently during emergencies. |
| Incident Report & Response + Localization | dispatch-service (port 8003) | Merged — both are about "who goes where." Separating them would create tight coupling with no independent scaling benefit. |
| Alert Management + Alert Delivery | notification-service (port 8004) | The zone-detection decision (Alert Management) lives in dispatch-service; only the delivery action lives here. Split justification: separating "who to notify" from "how to notify" keeps notification-service replaceable (swap SMS provider without touching dispatch logic). |
| Feedback & Review | Out of scope | Deferred — not required for MVP. Would be a future sixth service. |
| Analytics & Statistics | analytics-service (port 8005) | Standalone sink — consumes all event streams and serves the police dashboard. Isolated so analytics processing never blocks saga-critical services. |


## 5. Architectural Style — Choice & Justification (Lecture 2)

The prescribed style is Microservices + Event-Driven Architecture (EDA). Below is a defence against two alternatives.

### Chosen style: Microservices + Event-Driven
Each business capability is a separate deployable service. Services communicate exclusively through Apache Kafka topics (no synchronous HTTP between services). This satisfies ASR-1 (sub-second delivery), 
ASR-2 (isolated concurrency control per service), and ASR-3 (stateless 
pods that scale independently).

---

### Alternative 1: Monolithic Architecture
**Could it satisfy our ASRs?**
Partially. A monolith could satisfy Integrity (single process, single DB, no race conditions). However it would fail Scalability — the entire application must scale as one unit even if only the SOS endpoint is under load. It would also fail the sub-second Reliability requirement under high load because a slow analytics query in the same process could delay notification delivery.

**Dominant trade-off:** Simple to develop, impossible to scale selectively. A single bug in analytics could crash the entire emergency dispatch system unacceptable for a life-critical application.

---

### Alternative 2: Service-Oriented Architecture (SOA) with synchronous HTTP
**Could it satisfy our ASRs?**
It would satisfy Scalability (separate services). However it would break 
ASR-1 — synchronous HTTP chains (SOS calls dispatch, dispatch calls notification) add cumulative network latency and create tight coupling. If notification-service is down, the entire chain fails synchronously.

**Dominant trade-off:** Familiar request/response model, but tight coupling and latency accumulation make the 1-second constraint structurally unachievable at scale. EDA decouples producers from consumers so each hop is non-blocking.

## 6. Architectural Patterns Applied (Lecture 3 material)

Full pattern documentation is in patterns-template.md (Part H). 
Summary below.

| # | Pattern | File : approx. line | Problem solved in HELEP |
|---|---------|---------------------|-------------------------|
| 1 | Choreographed Saga | sos-service/app/main.py → dispatch-service/app/main.py → notification-service/app/main.py | Coordinates a multi-step business transaction (trigger → assign → notify) across 3 services without a central orchestrator |
| 2 | Pub/Sub via Kafka | all services: app/events.py | Decouples producers from consumers; enables async delivery that satisfies the sub-second ASR |
| 3 | Repository | all services: app/db.py | Isolates SQL from route handlers; makes the persistence layer swappable |
| 4 | Strategy | dispatch-service/app/matching.py : 31–75 | Allows the responder-selection algorithm to be swapped at runtime via MATCHER env var (nearest / credibility / roundrobin) |
| 5 | Outbox-lite | sos-service/app/main.py trigger() | DB write and Kafka publish happen in the same async block, reducing the window for lost events |
| 6 | Circuit Breaker | all services: app/events.py : 57–88 | Prevents cascading failures — when Kafka is unreachable, the breaker opens and stops flooding the broker with retries |
| 7 | Idempotency Key | notification-service/app/db.py : UNIQUE(incident_id, template) | Prevents duplicate SMS when Kafka redelivers a message after a consumer crash |
| 8 | Retry with Exponential Backoff | all services: app/events.py consume() | Handles transient handler failures gracefully before leaving a message uncommitted for redelivery |


## 7. Architecture Decision Records (ADRs)

### ADR-001: Kafka partition keying strategy

#### Context
Kafka topics can have multiple partitions. Events can be distributed across partitions in different ways. With multiple dispatch-service replicas consuming the same topic, ordering of events per incident matters a "sos.cancelled" must be processed after "sos.triggered" for the same incident, or a responder could be wrongly dispatched after a cancellation.

#### Decision
All saga-critical events are published with key=incident_id. Kafka guarantees that all messages with the same key land on the same partition. Since one partition is owned by exactly one consumer at a time within a group, all events for a single incident are always processed in order by the same pod.
We use 3 partitions per topic to allow up to 3 consumer pods to process different incidents in parallel.

#### Consequences
- Ordering per incident is guaranteed even with multiple replicas
- Maximum parallelism is bounded by partition count (3 pods max per topic)
- Hot partitions possible if one incident_id generates unusually high volume

#### Alternatives Considered
Round-robin partitioning (no key): fast but breaks ordering — rejected because it violates the no-double-dispatch constraint.

---

### ADR-002: SQLite per service vs shared PostgreSQL

#### Context
Each service needs a database to store its state (users, incidents, assignments, notifications, analytics). The choices are a shared PostgreSQL instance or SQLite embedded in each service container.

#### Decision
SQLite per service, stored on a Kubernetes PersistentVolumeClaim.
Each service owns its data and is the only writer. This enforces the microservices principle of loose coupling — no service can query another service's database directly.

#### Consequences
- No network hop for database queries (SQLite is in-process)
- Services are fully independent; one DB crash does not affect others
- ReadWriteOnce PVC limits horizontal scaling to 1 replica per service
- SQLite is not suitable for write-heavy production loads at scale

#### Alternatives Considered
Shared PostgreSQL: gives full ACID guarantees and supports multiple replicas — but couples all services to a single database, creating a shared point of failure and violating service isolation. Rejected for this project scope.

---

### ADR-003: Flat Kubernetes manifests vs Helm umbrella chart

#### Context
Kubernetes manifests for 5 services (35+ YAML files) can be managed as plain files or wrapped in a Helm chart that templates repeated values (namespace, image registry, replica count) into a single values.yaml file.

#### Decision
Flat YAML manifests organised in a k8s/ folder with one subfolder per service. Each file is explicit and self-contained.

#### Consequences
- Files are readable and understandable without knowing Helm
- No templating abstraction — changing the image registry requires editing multiple files
- Suitable for a 5-service project with a clear structure

#### Alternatives Considered
Helm umbrella chart with sub-charts: reduces repetition significantly and is the production standard. Would be the natural next step for this project as it grows. Rejected here to keep the submission focused on architecture rather than Helm tooling.


## 8. Trade-offs & Improvement Perspectives

### Weakness 1: SQLite limits true horizontal scaling
**Problem:** Each service's SQLite database is mounted via a ReadWriteOnce PVC, meaning only one pod can write to it at a time. The HPA can define up to 5 replicas, but in practice only 1 pod per service can hold the PVC mount. This makes the scaling strategy largely theoretical for the persistence layer.

**Proposed fix:** Replace SQLite with PostgreSQL (one instance per service, deployed as a StatefulSet). PostgreSQL supports concurrent connections from multiple pods. The Repository pattern already abstracts the database layer in each service's db.py, so the change would be isolated to that file — the rest of the service code is unaffected.

---

### Weakness 2: Single Kafka broker is a single point of failure
**Problem:** The Strimzi cluster is configured with 1 broker and 1 ZooKeeper node. If the Kafka pod crashes, all event-driven communication stops — no SOS events reach dispatch, no notifications are sent. This is the most critical failure point in the system.

**Proposed fix:** Increase to 3 Kafka broker replicas and 3 ZooKeeper replicas in kafka-cluster.yaml, and set replication.factor=3 on all topics. This tolerates the loss of any single broker without data loss or downtime.

---

### Weakness 3: Simulated SMS is not real delivery
**Problem:** The notification-service logs a structured line and writes a database row instead of sending a real SMS. In a real emergency, this means no one is actually notified outside the system.

**Proposed fix:** Integrate a real SMS gateway such as Twilio or Africa's Talking (which has Cameroon coverage). The notification-service already isolates the delivery logic inside on_event() — adding an HTTP call to the gateway API would be a local change with no impact on the rest of the architecture. The JWT_SECRET and gateway API key would both be stored as Kubernetes Secrets.


## 9. Submission checklist

- [ ] Every section above completed
- [ ] At least 3 diagrams (mermaid / drawio / hand-drawn scan acceptable)
- [ ] Every choice traced to an SRS line, an NFR, or an ASR
- [ ] 3 ADRs included
- [ ] Word count ~2000–3000
