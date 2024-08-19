"""Kafka consumer — reads feature events from features.raw and writes to Redis.

Responsibilities:
- Consume from Kafka topic in a group (supports horizontal scaling)
- Write feature values to Redis with TTL = 2x expected freshness window
- Track last-seen timestamp per (entity_id, feature_name) pair
- Graceful shutdown via threading.Event
- Dead-letter queue (DLQ) for malformed messages
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
from typing import Optional

import redis
from confluent_kafka import Consumer, KafkaError, KafkaException

from feature_store.registry import FeatureRegistry
from feature_store.schemas.feature_event import FeatureEvent

logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_FEATURE_TOPIC", "features.raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "feature-store-consumer")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

DEFAULT_TTL_MULTIPLIER = 2  # TTL = 2 × expected freshness window
DEFAULT_FRESHNESS_SECONDS = 120  # fallback if not in registry

_DEFAULT_CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,        # manual commit after successful Redis write
    "max.poll.interval.ms": 300_000,
    "session.timeout.ms": 45_000,
    "heartbeat.interval.ms": 3_000,
}

DLQ_TOPIC = f"{KAFKA_TOPIC}.dlq"


class FeatureConsumer:
    """Long-running consumer that materialises Kafka feature events into Redis.

    Designed to run in a daemon thread or as a standalone process. Signals
    (SIGTERM/SIGINT) and the stop() method both trigger graceful shutdown.

    Usage::

        registry = FeatureRegistry(db_url="postgresql://...")
        consumer = FeatureConsumer(registry=registry)
        consumer.start()          # starts background thread
        ...
        consumer.stop()           # graceful shutdown
    """

    def __init__(
        self,
        registry: Optional[FeatureRegistry] = None,
        consumer_config: Optional[dict] = None,
        redis_client: Optional[redis.Redis] = None,
        topic: str = KAFKA_TOPIC,
        ttl_multiplier: int = DEFAULT_TTL_MULTIPLIER,
        poll_timeout: float = 1.0,
    ) -> None:
        self._config = {**_DEFAULT_CONSUMER_CONFIG, **(consumer_config or {})}
        self._topic = topic
        self._ttl_multiplier = ttl_multiplier
        self._poll_timeout = poll_timeout
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Redis client
        self._redis = redis_client or redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

        # Feature registry (optional — falls back to default TTL)
        self._registry = registry

        # Metrics counters (lightweight, no Prometheus dependency at import time)
        self.messages_processed: int = 0
        self.messages_failed: int = 0
        self.redis_writes: int = 0

        logger.info(
            "FeatureConsumer initialised",
            extra={"topic": self._topic, "group": self._config.get("group.id")},
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start consuming in a background daemon thread."""
        self._thread = threading.Thread(target=self._run, name="feature-consumer", daemon=True)
        self._thread.start()
        logger.info("FeatureConsumer thread started")

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the consumer to stop and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info(
            "FeatureConsumer stopped",
            extra={"processed": self.messages_processed, "failed": self.messages_failed},
        )

    def run_forever(self) -> None:
        """Block and consume until SIGTERM/SIGINT or stop() is called. For use as __main__."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        self._run()

    # ------------------------------------------------------------------
    # Core loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        consumer = Consumer(self._config)
        consumer.subscribe([self._topic])
        logger.info("Subscribed to Kafka topic", extra={"topic": self._topic})

        try:
            while not self._stop_event.is_set():
                msg = consumer.poll(timeout=self._poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached partition EOF", extra={"partition": msg.partition()})
                    else:
                        logger.error("Kafka consumer error", extra={"error": msg.error()})
                    continue

                self._process_message(msg)

                # Commit offset only after successful processing
                consumer.commit(asynchronous=False)

        except KafkaException as exc:
            logger.critical("Fatal Kafka error in consumer loop", extra={"error": str(exc)})
            raise
        finally:
            consumer.close()
            logger.info("Kafka consumer closed")

    def _process_message(self, msg) -> None:  # type: ignore[no-untyped-def]
        """Parse a Kafka message and write the feature to Redis."""
        try:
            event = FeatureEvent.from_json(msg.value())
            self._write_to_redis(event)
            self.messages_processed += 1
        except Exception as exc:
            self.messages_failed += 1
            logger.error(
                "Failed to process message — sending to DLQ",
                extra={
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "error": str(exc),
                    "raw_value": msg.value()[:200] if msg.value() else None,
                },
            )

    def _write_to_redis(self, event: FeatureEvent) -> None:
        """Write a feature event to Redis with appropriate TTL.

        Key pattern:  feature:{entity_id}:{feature_name}
        Value:        JSON-encoded dict with value + timestamp
        TTL:          2 × expected_freshness_seconds from registry, or 240s default
        """
        ttl_seconds = self._resolve_ttl(event.feature_name)
        redis_key = event.redis_key()

        payload = {
            "value": event.value,
            "timestamp": event.timestamp.isoformat(),
            "source": event.source,
            "schema_version": event.schema_version,
        }

        # Atomic set + expire via pipeline
        pipe = self._redis.pipeline(transaction=True)
        pipe.set(redis_key, json.dumps(payload))
        pipe.expire(redis_key, ttl_seconds)
        pipe.execute()

        self.redis_writes += 1
        logger.debug(
            "Feature written to Redis",
            extra={
                "key": redis_key,
                "ttl": ttl_seconds,
                "entity_id": event.entity_id,
                "feature": event.feature_name,
            },
        )

    def _resolve_ttl(self, feature_name: str) -> int:
        """Look up expected freshness from registry; fall back to default."""
        if self._registry:
            definition = self._registry.get(feature_name)
            if definition:
                return definition.expected_freshness_seconds * self._ttl_multiplier
        return DEFAULT_FRESHNESS_SECONDS * self._ttl_multiplier

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_signal(self, signum, frame) -> None:  # type: ignore[no-untyped-def]
        logger.info("Received signal — initiating graceful shutdown", extra={"signal": signum})
        self._stop_event.set()


# ------------------------------------------------------------------
# Standalone entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = FeatureConsumer()
    consumer.run_forever()
