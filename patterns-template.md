# L3 Patterns-in-Code Document — HELEP (Template)


## Part A — Pre-implemented patterns

### A.1 Choreographed Saga
**Where — full event trace:**

Step 1 — Citizen triggers SOS:
  sos-service/app/main.py:85
  insert_incident() writes to DB, then await publish("sos.triggered", ..., key=iid)
  The key=iid ensures all events for this incident land on the same Kafka partition.

Step 2 — Dispatch service picks up the event:
  dispatch-service/app/main.py:54-55
  on_event() routes "sos.triggered" to handle_sos()

Step 3 — Dispatch assigns a responder:
  dispatch-service/app/main.py:77-81
  After reserving the responder atomically, publishes "responder.assigned"

Step 4 — Notification service receives assignment:
  notification-service/app/main.py:52-71
  on_event() formats and logs the SMS, then publishes "notification.sent"

**Compensation step (saga rollback):**
  Trigger:  sos-service/app/main.py:109
            cancel_sos() publishes "sos.cancelled" with key=iid

  Handler:  dispatch-service/app/main.py:96-102
            handle_cancel() calls release_assignment(iid) — sets busy=0 on the responder row and marks assignment as RELEASED then publishes "responder.confirmed" with status=RELEASED

**What state is rolled back:** The responder's busy flag is reset to 0, making them available for new assignments. The assignment row is marked RELEASED so idempotent re-processing of the same cancel event has no effect.

**Rollback trigger event:** sos.cancelled (produced at sos-service/app/main.py:109)


### A.2 Pub/Sub via Apache Kafka
**Where:** app/events.py in every service (producer() and consume() functions)

**At-least-once delivery + manual commit:**
  File: any service's app/events.py, consume() function
  auto_commit is disabled (enable_auto_commit=False). The consumer only calls
  await consumer.commit() AFTER the handler returns successfully. If the handler
  raises an exception, the offset is not committed — Kafka will redeliver the same message to the next available consumer in the group. This guarantees every event is processed at least once, even across pod restarts.

**Partition keying:**
  File: app/events.py, publish() function — key parameter
  Every saga-critical publish passes key=incident_id (e.g. sos-service/app/main.py:95).
  Kafka hashes the key to determine the partition. Same key → same partition.
  Since one partition is consumed by exactly one pod at a time within a consumer group, all events for a single incident are processed in order by the same pod.
  This is what prevents a "sos.cancelled" being processed before "sos.triggered" for the same incident, preserving the no-double-dispatch invariant.


### A.3 Repository
**Where:** app/db.py in every service

Examples:
  user-service/app/db.py   — functions: insert_user(), get_by_phone(), update_credibility()
  dispatch-service/app/db.py — functions: all_free_responders(), reserve_responder_for(), 
                                          release_assignment()
  notification-service/app/db.py — functions: record(), list_all()

**Why — what would break without it:**
  If route handlers queried SQLite directly (e.g. c.execute("SELECT...") inside a FastAPI endpoint), the database engine would be coupled to every route function.
  Replacing SQLite with PostgreSQL (a necessary upgrade for production, as noted in the trade-offs section) would require editing every single route handler.
  With the Repository pattern, only db.py changes all route handlers and Kafka handlers remain untouched.


### A.4 Strategy
**Where:** dispatch-service/app/matching.py

Three concrete strategies:
  NearestMatcher            matching.py:31-39   picks the geographically closest responder
  CredibilityWeightedMatcher matching.py:43-54  balances distance and credibility score
  RoundRobinMatcher         matching.py:57-68   cycles through responders in sequence

**How to switch at runtime:**
  matching.py:73-80 the matcher() factory function reads the MATCHER environment variable and returns the corresponding strategy object. Set in k8s/dispatch-service/configmap.yaml as MATCHER=nearest (or credibility or roundrobin).
  No code change required to switch algorithm — only a ConfigMap update and pod restart.

**Third strategy added (RoundRobinMatcher):**
  matching.py:57-68
  Ignores location entirely and cycles through all available responders in sequence.
  Useful when all responders are equidistant (e.g. small geographic zone) and even workload distribution matters more than proximity.


### A.5 Outbox-lite
**Where:** sos-service/app/main.py, trigger() function, lines 84-96

  Line 84: insert_incident(iid, ...) — writes the incident row to SQLite
  Lines 85-96: await publish("sos.triggered", {...}, key=iid) — publishes to Kafka

Both operations happen in the same async block with no await between them (insert is synchronous SQLite, publish is the next line).

**Why this is "lite" — what a real Outbox adds:**
  A real Transactional Outbox pattern uses a database transaction that atomically writes BOTH the business row AND an outbox row in the same commit. A separate polling process reads the outbox table and publishes to Kafka, then deletes the row. This guarantees exactly-once publishing even if the process crashes between the DB write and the Kafka send.
  
  Our implementation is "lite" because the two operations are NOT in a single database transaction. If the process crashes after insert_incident() but before publish(), the incident is saved but the Kafka event is never sent the SOS is lost silently. Acceptable for this project scope; a real system would use the full pattern.


### A.6 Circuit Breaker (stub → complete it)
**Where:** app/events.py in every service, CircuitBreaker class (lines 57–88)

**Three states and transitions:**

CLOSED (normal operation):
  allow() returns True. Every Kafka publish attempt passes through.
  record_failure() increments self.fails.
  When self.fails >= fail_threshold (default 5):
    → transition to OPEN, record self.opened_at = time.monotonic()

OPEN (broker unreachable — fast fail):
  allow() returns False immediately — no publish attempt is made.
  This prevents the service from flooding a dead Kafka broker with retries.
  Every 10 seconds (reset_after_s), allow() checks if the timeout has passed:
    If yes → transition to HALF_OPEN, return True (one probe call allowed)
    If no  → return False (still blocked)

HALF_OPEN (probing — one test call):
  allow() returns True once, letting one publish attempt through.
  If that publish succeeds: record_success() → self.fails = 0, → CLOSED
  If that publish fails:   record_failure() → immediately back to OPEN

**What triggers state transitions in this implementation:**
  CLOSED → OPEN:     5 consecutive publish() failures (lines 84-88)
  OPEN → HALF_OPEN:  10 seconds elapsed since opened_at (line 64-66)
  HALF_OPEN → CLOSED: record_success() called after a successful send (lines 90-92)
  HALF_OPEN → OPEN:   record_failure() called when the probe fails (lines 94-97)


## Part B — Patterns you added (minimum 2)

### B.1 Idempotency Key
**Pattern name:** Idempotency Key (Cloud-Native / EAA catalogue)

**Where added:**
  notification-service/app/db.py — init() function
  UNIQUE(incident_id, template) constraint on the notifications table (line ~33)
  INSERT OR IGNORE in record() function (line ~41)
  
  notification-service/app/main.py:63
  record() is called with incident_id=p.get("incident_id") so the key is always stored.

**Problem it solves in HELEP:**
  Kafka guarantees at-least-once delivery. If a notification-service pod crashes after processing an event but before committing the Kafka offset, Kafka redelivers the same event on restart. Without this pattern, the citizen or responder receives duplicate SMS messages for the same incident — a serious UX failure in an emergency system.
  With UNIQUE(incident_id, template), the second INSERT OR IGNORE on the same (incident_id, template) pair is silently dropped. The message is never sent twice.

**Trade-off vs alternative:**
  Alternative: check-then-insert (SELECT first, INSERT if not found).
  Problem: this is a read-modify-write race condition — two pods could both SELECT "not found" and both INSERT, defeating the protection.
  The UNIQUE constraint + INSERT OR IGNORE is atomic at the database level no race condition possible.


### B.2 Retry with Exponential Backoff
**Pattern name:** Retry with Exponential Backoff (Cloud-Native / Resilience catalogue)

**Where added:**
  app/events.py — consume() function in every service
  The for attempt in range(3) loop with await asyncio.sleep(2 ** attempt)

**Problem it solves in HELEP:**
  Transient failures (brief DB lock, momentary network blip) would previously cause the event handler to fail immediately, leaving the message uncommitted. Kafka would redeliver it, but only after the consumer group rebalance timeout potentially seconds later. Under emergency load, this delay is unacceptable.
  
  With retry + backoff: attempt 0 runs immediately, attempt 1 waits 1 second, 
  attempt 2 waits 2 seconds. Transient errors resolve within 3 seconds without any message loss or rebalance. Only persistent errors exhaust all 3 attempts and leave the offset uncommitted for Kafka redelivery.

**Trade-off vs alternative:**
  Alternative: infinite retry loop.
  Problem: a persistent failure (e.g. corrupt message payload) would cause the consumer to loop forever, blocking all subsequent messages on that partition.
  Three attempts with backoff bounds the worst case at ~3 seconds, then the message is left for redelivery — a deliberate "let it fail" decision that keeps the consumer moving.



## Part C — Anti-patterns avoided

**Anti-pattern: Shared Database across services**

The HELEP architecture explicitly avoids the Shared Database anti-pattern,where multiple services read and write the same database schema directly.

**Where avoidance is demonstrated:**
  Each service has a completely separate SQLite file and db.py module:
  - user-service/app/db.py      → /data/user.db
  - sos-service/app/db.py       → /data/sos.db
  - dispatch-service/app/db.py  → /data/dispatch.db
  - notification-service/app/db.py → /data/notification.db
  - analytics-service/app/db.py → /data/analytics.db

  No service imports another service's db.py. If dispatch-service needs to know about a user, it reads from the Kafka event payload — not from user-service's database.

**Why this anti-pattern is dangerous:**
  A shared database creates invisible coupling. If user-service changes a table column, dispatch-service's queries silently break at runtime with no compile-time warning. It also means one service's heavy read load (analytics running a zone aggregation query) can starve another service's writes (dispatch trying to claim a responder) breaking the sub-1-second ASR.
  
  Service-isolated databases ensure each service owns its schema completely and scales its persistence independently.


