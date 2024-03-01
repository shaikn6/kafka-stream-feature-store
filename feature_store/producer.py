"""Kafka producer — publish feature events to the features.raw topic.

Responsibilities:
- Schema validation via Pydantic before any message is sent
- Idempotent delivery with configurable retries and backoff
- Synchronous (blocking) and fire-and-forget (async callback) modes
- Structured logging for observability
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable, List, Optional

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from feature_store.schemas.feature_event import FeatureEvent

logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_FEATURE_TOPIC", "features.raw")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Default producer config following Confluent best practices for exactly-once-style reliability
_DEFAULT_PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",                    # wait for all ISR replicas
    "retries": 5,
    "retry.backoff.ms": 200,
    "enable.idempotence": True,       # prevents duplicate messages on retry
    "compression.type": "snappy",
    "linger.ms": 5,                   # small batching window for throughput
    "batch.size": 65536,
}


def _delivery_report(err, msg) -> None:  # type: ignore[no-untyped-def]
    """Default delivery callback — logs success or failure."""
    if err:
        logger.error(
            "Message delivery failed",
            extra={"topic": msg.topic(), "partition": msg.partition(), "error": str(err)},
        )
    else:
        logger.debug(
            "Message delivered",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )


class FeatureProducer:
    """Thread-safe Kafka producer for feature events.

    Usage::

        producer = FeatureProducer()
        event = FeatureEvent(entity_id="order_123", feature_name="rolling_7d_spend", value=142.5)
        producer.publish(event)
        producer.flush()
    """

    def __init__(
        self,
        config: Optional[dict] = None,
        topic: str = KAFKA_TOPIC,
        on_delivery: Optional[Callable] = None,
    ) -> None:
        self._config = {**_DEFAULT_PRODUCER_CONFIG, **(config or {})}
        self._topic = topic
        self._on_delivery = on_delivery or _delivery_report
        self._producer = Producer(self._config)
        logger.info("FeatureProducer initialised", extra={"bootstrap": self._config["bootstrap.servers"]})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, event: FeatureEvent, key: Optional[str] = None) -> None:
        """Serialize and publish a single FeatureEvent.

        Args:
            event: Validated FeatureEvent instance.
            key:   Optional Kafka message key (defaults to entity_id for partitioning).

        Raises:
            KafkaException: If the internal Kafka queue is full after retries.
            ValueError:     If the event fails schema validation (caught by Pydantic).
        """
        message_key = (key or event.entity_id).encode("utf-8")
        message_value = event.to_json().encode("utf-8")

        retry_count = 0
        max_retries = 3
        while retry_count <= max_retries:
            try:
                self._producer.produce(
                    topic=self._topic,
                    key=message_key,
                    value=message_value,
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)  # trigger callbacks without blocking
                logger.debug(
                    "Published feature event",
                    extra={
                        "entity_id": event.entity_id,
                        "feature_name": event.feature_name,
                        "topic": self._topic,
                    },
                )
                return
            except BufferError:
                # Internal queue is full — flush and retry
                retry_count += 1
                logger.warning(
                    "Kafka producer buffer full, flushing and retrying",
                    extra={"retry": retry_count},
                )
                self._producer.flush(timeout=5)
                time.sleep(0.1 * retry_count)

        raise KafkaException(f"Failed to produce message after {max_retries} retries (buffer exhausted)")

    def publish_batch(self, events: List[FeatureEvent]) -> int:
        """Publish a list of FeatureEvents. Returns number successfully enqueued."""
        published = 0
        for event in events:
            try:
                self.publish(event)
                published += 1
            except Exception as exc:
                logger.error(
                    "Failed to publish event",
                    extra={"entity_id": event.entity_id, "feature": event.feature_name, "error": str(exc)},
                )
        return published

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all pending messages. Returns number still in queue (should be 0)."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("Flush timed out with messages still pending", extra={"remaining": remaining})
        return remaining

    def close(self) -> None:
        """Flush and close the producer."""
        self.flush()
        logger.info("FeatureProducer closed")

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "FeatureProducer":
        return self

    def __exit__(self, *_) -> None:  # type: ignore[no-untyped-def]
        self.close()


# ------------------------------------------------------------------
# Admin helpers
# ------------------------------------------------------------------

def ensure_topic_exists(
    topic: str = KAFKA_TOPIC,
    num_partitions: int = 3,
    replication_factor: int = 1,
    bootstrap_servers: str = KAFKA_BOOTSTRAP,
) -> None:
    """Create the Kafka topic if it does not already exist.

    Safe to call repeatedly — silently ignores TopicAlreadyExistsException.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
    futures = admin.create_topics([new_topic])
    for t, future in futures.items():
        try:
            future.result()
            logger.info("Topic created", extra={"topic": t})
        except Exception as exc:
            if "already exists" in str(exc).lower() or "topic_already_exists" in str(exc).lower():
                logger.debug("Topic already exists", extra={"topic": t})
            else:
                logger.error("Failed to create topic", extra={"topic": t, "error": str(exc)})
                raise
