"""Unit tests for feature_store.consumer.

Uses fakeredis for Redis and a hand-crafted Kafka message mock so no
real broker or Redis server is required.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from feature_store.consumer import FeatureConsumer
from feature_store.schemas.feature_event import FeatureDefinition, FeatureEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_kafka_message(event: FeatureEvent) -> MagicMock:
    """Return a mock confluent_kafka.Message for the given FeatureEvent."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = event.to_json().encode("utf-8")
    msg.partition.return_value = 0
    msg.offset.return_value = 1
    return msg


def make_bad_message() -> MagicMock:
    """Return a mock Kafka message with corrupted payload."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = b"not valid json {{{"
    msg.partition.return_value = 0
    msg.offset.return_value = 99
    return msg


class MockRegistry:
    """Minimal in-memory feature registry for testing."""

    def __init__(self) -> None:
        self._features = {
            "rolling_7d_spend": FeatureDefinition(
                feature_name="rolling_7d_spend",
                description="test",
                owner="test",
                expected_freshness_seconds=45,
                value_type="float",
            ),
            "order_count_24h": FeatureDefinition(
                feature_name="order_count_24h",
                description="test",
                owner="test",
                expected_freshness_seconds=60,
                value_type="int",
            ),
        }

    def get(self, feature_name: str):
        return self._features.get(feature_name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_redis():
    """Return a fakeredis server-backed Redis client."""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    return client


@pytest.fixture()
def registry():
    return MockRegistry()


@pytest.fixture()
def consumer(fake_redis, registry):
    return FeatureConsumer(
        registry=registry,
        redis_client=fake_redis,
        ttl_multiplier=2,
    )


@pytest.fixture()
def sample_event():
    return FeatureEvent(
        entity_id="customer_001",
        feature_name="rolling_7d_spend",
        value=99.99,
        timestamp=datetime(2022, 6, 15, 10, 0, 0),
        source="unit-test",
    )


# ---------------------------------------------------------------------------
# Redis write tests
# ---------------------------------------------------------------------------

class TestRedisWrite:
    def test_write_sets_correct_key(self, consumer, fake_redis, sample_event):
        consumer._write_to_redis(sample_event)
        expected_key = "feature:customer_001:rolling_7d_spend"
        assert fake_redis.exists(expected_key)

    def test_write_stores_value_in_payload(self, consumer, fake_redis, sample_event):
        consumer._write_to_redis(sample_event)
        raw = fake_redis.get("feature:customer_001:rolling_7d_spend")
        payload = json.loads(raw)
        assert payload["value"] == 99.99

    def test_write_stores_timestamp(self, consumer, fake_redis, sample_event):
        consumer._write_to_redis(sample_event)
        raw = fake_redis.get("feature:customer_001:rolling_7d_spend")
        payload = json.loads(raw)
        assert "timestamp" in payload
        assert "2022-06-15" in payload["timestamp"]

    def test_write_sets_ttl_from_registry(self, consumer, fake_redis, sample_event):
        """TTL should be 2 * 45 = 90 seconds for rolling_7d_spend."""
        consumer._write_to_redis(sample_event)
        key = "feature:customer_001:rolling_7d_spend"
        ttl = fake_redis.ttl(key)
        # TTL should be 90s (give a small buffer for test execution time)
        assert 88 <= ttl <= 90

    def test_write_uses_default_ttl_for_unknown_feature(self, consumer, fake_redis):
        event = FeatureEvent(
            entity_id="customer_002",
            feature_name="unknown_feature",
            value=1.0,
        )
        consumer._write_to_redis(event)
        key = "feature:customer_002:unknown_feature"
        ttl = fake_redis.ttl(key)
        # Default: 120 * 2 = 240
        assert 238 <= ttl <= 240

    def test_write_overwrites_existing_value(self, consumer, fake_redis, sample_event):
        consumer._write_to_redis(sample_event)
        updated = FeatureEvent(
            entity_id="customer_001",
            feature_name="rolling_7d_spend",
            value=200.0,
        )
        consumer._write_to_redis(updated)
        raw = fake_redis.get("feature:customer_001:rolling_7d_spend")
        payload = json.loads(raw)
        assert payload["value"] == 200.0


# ---------------------------------------------------------------------------
# Message processing tests
# ---------------------------------------------------------------------------

class TestMessageProcessing:
    def test_valid_message_increments_counter(self, consumer, sample_event):
        msg = make_kafka_message(sample_event)
        consumer._process_message(msg)
        assert consumer.messages_processed == 1
        assert consumer.messages_failed == 0

    def test_invalid_message_increments_failed_counter(self, consumer):
        msg = make_bad_message()
        consumer._process_message(msg)
        assert consumer.messages_failed == 1
        assert consumer.messages_processed == 0

    def test_valid_message_writes_to_redis(self, consumer, fake_redis, sample_event):
        msg = make_kafka_message(sample_event)
        consumer._process_message(msg)
        key = "feature:customer_001:rolling_7d_spend"
        assert fake_redis.exists(key)

    def test_multiple_entities_stored_independently(self, consumer, fake_redis):
        for i in range(5):
            event = FeatureEvent(
                entity_id=f"customer_{i:03d}",
                feature_name="order_count_24h",
                value=i,
            )
            msg = make_kafka_message(event)
            consumer._process_message(msg)

        assert consumer.messages_processed == 5
        for i in range(5):
            key = f"feature:customer_{i:03d}:order_count_24h"
            assert fake_redis.exists(key), f"Missing key: {key}"


# ---------------------------------------------------------------------------
# TTL resolution
# ---------------------------------------------------------------------------

class TestTTLResolution:
    def test_known_feature_uses_registry_ttl(self, consumer, registry):
        ttl = consumer._resolve_ttl("rolling_7d_spend")
        assert ttl == 45 * 2

    def test_unknown_feature_uses_default_ttl(self, consumer):
        ttl = consumer._resolve_ttl("nonexistent_feature")
        assert ttl == 120 * 2  # DEFAULT_FRESHNESS_SECONDS * multiplier

    def test_no_registry_uses_default(self, fake_redis):
        c = FeatureConsumer(registry=None, redis_client=fake_redis)
        ttl = c._resolve_ttl("any_feature")
        assert ttl == 120 * 2


# ---------------------------------------------------------------------------
# Stop event
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_stop_event_set_on_stop(self, consumer):
        consumer._stop_event.clear()
        consumer.stop(timeout=0.1)
        assert consumer._stop_event.is_set()
