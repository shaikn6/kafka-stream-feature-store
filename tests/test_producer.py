"""Unit tests for feature_store.producer.

Kafka is mocked via pytest-mock so no broker is required.
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from feature_store.producer import FeatureProducer, _delivery_report
from feature_store.schemas.feature_event import FeatureEvent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_kafka_producer():
    """Return a MagicMock that stands in for confluent_kafka.Producer."""
    with patch("feature_store.producer.Producer") as MockProducer:
        mock_instance = MagicMock()
        MockProducer.return_value = mock_instance
        yield mock_instance


@pytest.fixture()
def producer(mock_kafka_producer):
    """FeatureProducer wired to a mocked Kafka Producer."""
    return FeatureProducer(config={"bootstrap.servers": "localhost:9092"})


@pytest.fixture()
def sample_event():
    return FeatureEvent(
        entity_id="order_001",
        feature_name="rolling_7d_spend",
        value=142.5,
        timestamp=datetime(2022, 6, 15, 12, 0, 0),
        source="unit-test",
    )


# ---------------------------------------------------------------------------
# Schema / serialisation tests
# ---------------------------------------------------------------------------


class TestFeatureEventSchema:
    def test_valid_event_serialises_to_json(self, sample_event):
        payload = json.loads(sample_event.to_json())
        assert payload["entity_id"] == "order_001"
        assert payload["feature_name"] == "rolling_7d_spend"
        assert payload["value"] == 142.5

    def test_roundtrip_from_json(self, sample_event):
        raw = sample_event.to_json()
        restored = FeatureEvent.from_json(raw)
        assert restored.entity_id == sample_event.entity_id
        assert restored.feature_name == sample_event.feature_name
        assert restored.value == sample_event.value

    def test_roundtrip_from_bytes(self, sample_event):
        raw = sample_event.to_json().encode("utf-8")
        restored = FeatureEvent.from_json(raw)
        assert restored.entity_id == sample_event.entity_id

    def test_feature_name_must_be_lowercase(self):
        with pytest.raises(Exception):
            FeatureEvent(entity_id="x", feature_name="Rolling_7d_spend", value=1.0)

    def test_feature_name_must_be_alphanumeric_underscores(self):
        with pytest.raises(Exception):
            FeatureEvent(entity_id="x", feature_name="feature-name!", value=1.0)

    def test_entity_id_must_not_be_empty(self):
        with pytest.raises(Exception):
            FeatureEvent(entity_id="  ", feature_name="some_feature", value=1.0)

    def test_redis_key_format(self, sample_event):
        assert sample_event.redis_key() == "feature:order_001:rolling_7d_spend"

    def test_value_can_be_int(self):
        event = FeatureEvent(entity_id="u1", feature_name="order_count_24h", value=5)
        assert event.value == 5

    def test_value_can_be_string(self):
        event = FeatureEvent(
            entity_id="u1", feature_name="preferred_category", value="groceries"
        )
        assert event.value == "groceries"

    def test_value_can_be_bool(self):
        event = FeatureEvent(entity_id="u1", feature_name="is_premium_user", value=True)
        assert event.value is True


# ---------------------------------------------------------------------------
# Producer publish tests
# ---------------------------------------------------------------------------


class TestFeatureProducer:
    def test_publish_calls_produce_with_correct_topic(
        self, producer, mock_kafka_producer, sample_event
    ):
        producer.publish(sample_event)
        mock_kafka_producer.produce.assert_called_once()
        call_kwargs = mock_kafka_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "features.raw"

    def test_publish_uses_entity_id_as_key(
        self, producer, mock_kafka_producer, sample_event
    ):
        producer.publish(sample_event)
        call_kwargs = mock_kafka_producer.produce.call_args
        assert call_kwargs.kwargs["key"] == b"order_001"

    def test_publish_accepts_custom_key(
        self, producer, mock_kafka_producer, sample_event
    ):
        producer.publish(sample_event, key="partition_key_42")
        call_kwargs = mock_kafka_producer.produce.call_args
        assert call_kwargs.kwargs["key"] == b"partition_key_42"

    def test_publish_value_is_valid_json(
        self, producer, mock_kafka_producer, sample_event
    ):
        producer.publish(sample_event)
        call_kwargs = mock_kafka_producer.produce.call_args
        raw_value = call_kwargs.kwargs["value"]
        payload = json.loads(raw_value.decode("utf-8"))
        assert payload["entity_id"] == "order_001"
        assert payload["feature_name"] == "rolling_7d_spend"

    def test_publish_batch_returns_count(self, producer, mock_kafka_producer):
        events = [
            FeatureEvent(entity_id=f"user_{i}", feature_name="order_count_24h", value=i)
            for i in range(5)
        ]
        count = producer.publish_batch(events)
        assert count == 5
        assert mock_kafka_producer.produce.call_count == 5

    def test_flush_called_on_close(self, producer, mock_kafka_producer):
        mock_kafka_producer.flush.return_value = 0
        producer.close()
        mock_kafka_producer.flush.assert_called_once()

    def test_context_manager_flushes_on_exit(self, mock_kafka_producer):
        mock_kafka_producer.flush.return_value = 0
        with FeatureProducer(config={"bootstrap.servers": "localhost:9092"}) as prod:
            event = FeatureEvent(
                entity_id="u1", feature_name="order_count_24h", value=1
            )
            prod.publish(event)
        mock_kafka_producer.flush.assert_called()

    def test_publish_batch_continues_on_single_failure(
        self, producer, mock_kafka_producer
    ):
        """A failure on one event should not stop the rest of the batch."""
        mock_kafka_producer.produce.side_effect = [None, Exception("broker down"), None]
        events = [
            FeatureEvent(entity_id=f"u{i}", feature_name="order_count_24h", value=i)
            for i in range(3)
        ]
        count = producer.publish_batch(events)
        # 2 succeeded, 1 failed
        assert count == 2


# ---------------------------------------------------------------------------
# Delivery report callback
# ---------------------------------------------------------------------------


class TestDeliveryReport:
    def test_no_error_does_not_raise(self):
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "features.raw"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42
        _delivery_report(None, mock_msg)  # should not raise

    def test_error_does_not_raise(self):
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "features.raw"
        mock_msg.partition.return_value = 0
        _delivery_report(Exception("broker error"), mock_msg)  # logs but does not raise


# ---------------------------------------------------------------------------
# Edge cases: buffer error retry, flush warning
# ---------------------------------------------------------------------------

class TestProducerEdgeCases:
    def test_publish_retries_on_buffer_error_then_succeeds(self, producer, mock_kafka_producer):
        mock_kafka_producer.produce.side_effect = [BufferError(), None]
        mock_kafka_producer.flush.return_value = 0
        event = FeatureEvent(entity_id="u1", feature_name="order_count_24h", value=1)
        producer.publish(event)
        assert mock_kafka_producer.produce.call_count == 2

    def test_publish_raises_after_max_retries(self, producer, mock_kafka_producer):
        from confluent_kafka import KafkaException
        mock_kafka_producer.flush.return_value = 0
        mock_kafka_producer.produce.side_effect = BufferError()
        with pytest.raises(KafkaException):
            event = FeatureEvent(entity_id="u2", feature_name="order_count_24h", value=1)
            producer.publish(event)

    def test_flush_warns_on_remaining_messages(self, producer, mock_kafka_producer):
        mock_kafka_producer.flush.return_value = 5
        result = producer.flush(timeout=0.1)
        assert result == 5
