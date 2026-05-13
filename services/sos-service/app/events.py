"""Apache Kafka producer + consumer helpers (aiokafka).

Patterns:
  - Pub/Sub (Kafka topics + consumer groups)
  - Outbox-lite (publish + db-write live in same async block in main.py)
  - Circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED state machine)
  - Retry with exponential backoff (consume loop, up to 3 attempts)

Partition keying:
  Every saga-critical publish should pass key=<incident_id> (or <user_id>).
  Same-key events land on the same partition, preserving ordering and ensuring
  the "no double dispatch" invariant holds even with multi-replica consumers
  (one partition is owned by one consumer at a time inside a group).
"""
from __future__ import annotations
import asyncio
import json
import os
import time
from typing import Awaitable, Callable, Iterable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

_producer: AIOKafkaProducer | None = None


async def producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_idempotence=True,
            acks="all",
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
        await _producer.start()
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def health() -> bool:
    """Liveness ping for /readyz — verify broker reachable + metadata fetch works."""
    try:
        p = await producer()
        await p.client.fetch_all_metadata()
        return True
    except Exception:
        return False


# ---- Circuit breaker (CLOSED → OPEN → HALF_OPEN → CLOSED) ----
class CircuitBreaker:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, fail_threshold: int = 5, reset_after_s: float = 10.0):
        self.fail_threshold = fail_threshold
        self.reset_after_s = reset_after_s
        self.fails = 0
        self.opened_at: float | None = None
        self._state = self.CLOSED

    def allow(self) -> bool:
        if self._state == self.CLOSED:
            return True
        if self._state == self.OPEN:
            if self.opened_at and (time.monotonic() - self.opened_at) >= self.reset_after_s:
                self._state = self.HALF_OPEN
                return True  # one probe allowed through
            return False
        return True  # HALF_OPEN: one probe passes

    def record_success(self) -> None:
        self.fails = 0
        self._state = self.CLOSED
        self.opened_at = None

    def record_failure(self) -> None:
        self.fails += 1
        if self._state == self.HALF_OPEN or self.fails >= self.fail_threshold:
            self._state = self.OPEN
            self.opened_at = time.monotonic()


_breaker = CircuitBreaker()


async def publish(topic: str, event: dict, key: str | None = None) -> None:
    """Outbox-lite: caller should db-write THEN await publish() in same async block."""
    if not _breaker.allow():
        raise RuntimeError(f"circuit-open: {topic}")
    try:
        p = await producer()
        await p.send_and_wait(topic, value=event, key=key)
        _breaker.record_success()
    except Exception:
        _breaker.record_failure()
        raise


Handler = Callable[[dict], Awaitable[None]]


async def consume(topics: Iterable[str], group: str, handler: Handler) -> None:
    """Consumer-group reader. Manual commit only on successful handler (at-least-once).
    Pattern: Retry with exponential backoff — up to 3 attempts before leaving uncommitted.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = msg.value
            payload["_stream"] = msg.topic  # preserved name for back-compat with handlers
            for attempt in range(3):
                try:
                    await handler(payload)
                    await consumer.commit()
                    break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    # on final attempt leave un-committed → re-delivered (at-least-once)
    finally:
        await consumer.stop()
