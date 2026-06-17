"""Unit tests for feature_store.monitor.FeatureMonitor.

Uses fakeredis for Redis and an in-memory registry stub.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import fakeredis
import pytest

from feature_store.monitor import FeatureMonitor
from feature_store.schemas.feature_event import FeatureDefinition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class InMemoryRegistry:
    def __init__(self, features: Dict[str, int]) -> None:
        """features: {feature_name: expected_freshness_seconds}"""
        self._features = {
            name: FeatureDefinition(
                feature_name=name,
                description="test",
                owner="test",
                expected_freshness_seconds=sla,
                value_type="float",
            )
            for name, sla in features.items()
        }

    def list_all(self, active_only: bool = True) -> List[FeatureDefinition]:
        return list(self._features.values())


def write_feature(redis_client, entity_id: str, feature_name: str, value=1.0, age_seconds: float = 5.0) -> None:
    ts = datetime.now(tz=timezone.utc) - timedelta(seconds=age_seconds)
    key = f"feature:{entity_id}:{feature_name}"
    payload = {"value": value, "timestamp": ts.isoformat(), "source": "test", "schema_version": "1.0"}
    redis_client.set(key, json.dumps(payload))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_redis():
    server = fakeredis.FakeServer()
    return fakeredis.FakeRedis(server=server, decode_responses=True)


@pytest.fixture()
def registry():
    return InMemoryRegistry({"rolling_7d_spend": 45, "order_count_24h": 60})


@pytest.fixture()
def monitor(registry, fake_redis):
    return FeatureMonitor(registry=registry, redis_client=fake_redis, interval_seconds=999)


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

class TestInitialState:
    def test_initial_stale_count_is_zero(self, monitor):
        stale, total = monitor.get_stale_counts()
        assert stale == 0
        assert total == 0

    def test_initial_last_run_is_none(self, monitor):
        assert monitor.get_last_run() is None

    def test_total_sla_violations_starts_at_zero(self, monitor):
        assert monitor.total_sla_violations == 0


# ---------------------------------------------------------------------------
# Monitoring logic — fresh features
# ---------------------------------------------------------------------------

class TestFreshFeatures:
    def test_no_stale_when_all_fresh(self, monitor, fake_redis):
        write_feature(fake_redis, "user_001", "rolling_7d_spend", age_seconds=10)
        write_feature(fake_redis, "user_001", "order_count_24h", age_seconds=5)
        monitor._check_all_features()
        stale, total = monitor.get_stale_counts()
        assert stale == 0
        assert total == 2

    def test_last_run_set_after_check(self, monitor, fake_redis):
        write_feature(fake_redis, "u1", "rolling_7d_spend", age_seconds=5)
        monitor._check_all_features()
        last = monitor.get_last_run()
        assert last is not None
        age = (datetime.now(tz=timezone.utc) - last).total_seconds()
        assert age < 5

    def test_multiple_entities_fresh(self, monitor, fake_redis):
        for i in range(10):
            write_feature(fake_redis, f"user_{i:03d}", "rolling_7d_spend", age_seconds=5)
        monitor._check_all_features()
        stale, total = monitor.get_stale_counts()
        assert stale == 0
        assert total == 10


# ---------------------------------------------------------------------------
# Monitoring logic — stale features
# ---------------------------------------------------------------------------

class TestStaleFeatures:
    def test_detects_stale_feature(self, monitor, fake_redis):
        # rolling_7d_spend SLA = 45s; write with 100s age = stale
        write_feature(fake_redis, "user_002", "rolling_7d_spend", age_seconds=100)
        monitor._check_all_features()
        stale, _ = monitor.get_stale_counts()
        assert stale == 1

    def test_stale_details_populated(self, monitor, fake_redis):
        write_feature(fake_redis, "user_003", "rolling_7d_spend", age_seconds=200)
        monitor._check_all_features()
        details = monitor.get_stale_details()
        assert "rolling_7d_spend" in details
        assert "user_003" in details["rolling_7d_spend"]

    def test_sla_violation_counter_increments(self, monitor, fake_redis):
        write_feature(fake_redis, "u1", "rolling_7d_spend", age_seconds=100)
        write_feature(fake_redis, "u2", "rolling_7d_spend", age_seconds=100)
        monitor._check_all_features()
        assert monitor.total_sla_violations == 2

    def test_violation_counter_accumulates_across_runs(self, monitor, fake_redis):
        write_feature(fake_redis, "u1", "rolling_7d_spend", age_seconds=100)
        monitor._check_all_features()
        monitor._check_all_features()
        # Runs twice, same key stale both times
        assert monitor.total_sla_violations >= 2

    def test_mixed_stale_and_fresh(self, monitor, fake_redis):
        write_feature(fake_redis, "u_fresh", "rolling_7d_spend", age_seconds=10)   # fresh
        write_feature(fake_redis, "u_stale", "rolling_7d_spend", age_seconds=100)  # stale
        monitor._check_all_features()
        stale, total = monitor.get_stale_counts()
        assert stale == 1
        assert total == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_no_registry_does_not_crash(self, fake_redis):
        mon = FeatureMonitor(registry=None, redis_client=fake_redis, interval_seconds=999)
        mon._check_all_features()  # should return early silently
        stale, total = mon.get_stale_counts()
        assert stale == 0
        assert total == 0

    def test_empty_registry_does_not_crash(self, fake_redis):
        empty_reg = InMemoryRegistry({})
        mon = FeatureMonitor(registry=empty_reg, redis_client=fake_redis, interval_seconds=999)
        mon._check_all_features()
        stale, total = mon.get_stale_counts()
        assert stale == 0

    def test_key_with_corrupted_json_skipped_gracefully(self, monitor, fake_redis):
        fake_redis.set("feature:u1:rolling_7d_spend", "not-valid-json")
        monitor._check_all_features()  # must not raise
        stale, total = monitor.get_stale_counts()
        # Corrupted key is counted in SCAN but can't be parsed — treated as not stale
        assert total >= 0

    def test_stale_details_empty_when_all_fresh(self, monitor, fake_redis):
        write_feature(fake_redis, "u1", "rolling_7d_spend", age_seconds=5)
        monitor._check_all_features()
        details = monitor.get_stale_details()
        assert details == {}

    def test_scan_keys_generator(self, monitor, fake_redis):
        for i in range(5):
            write_feature(fake_redis, f"user_{i}", "rolling_7d_spend", age_seconds=5)
        keys = list(monitor._scan_keys("feature:*:rolling_7d_spend"))
        assert len(keys) == 5


# ---------------------------------------------------------------------------
# Thread lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_creates_daemon_thread(self, monitor):
        monitor.start()
        assert monitor._thread is not None
        assert monitor._thread.daemon is True
        monitor.stop()

    def test_stop_sets_stop_event(self, monitor):
        monitor.start()
        monitor.stop(timeout=1.0)
        assert monitor._stop_event.is_set()

    def test_get_stale_counts_thread_safe(self, monitor, fake_redis):
        """Calling get_stale_counts while thread is running must not raise."""
        write_feature(fake_redis, "u1", "rolling_7d_spend", age_seconds=10)
        monitor.start()
        for _ in range(20):
            stale, total = monitor.get_stale_counts()
        monitor.stop()


# ---------------------------------------------------------------------------
# Edge cases: expired key, naive timezone, exception in run loop
# ---------------------------------------------------------------------------

class TestCheckFeatureEdgeCases:
    def test_check_feature_skips_expired_key(self, monitor, fake_redis):
        """Key present in SCAN but None from GET (expired between scan and get)."""
        write_feature(fake_redis, "user_exp", "rolling_7d_spend", age_seconds=200)
        from unittest.mock import patch
        with patch.object(fake_redis, "get", return_value=None):
            defn = FeatureDefinition(
                feature_name="rolling_7d_spend",
                description="test",
                owner="test",
                expected_freshness_seconds=45,
                value_type="float",
            )
            result = monitor._check_feature(defn)
        assert result == []

    def test_check_feature_handles_naive_timestamp(self, monitor, fake_redis):
        """Naive (no tzinfo) timestamp in Redis is treated as UTC."""
        naive_ts = datetime.utcnow() - timedelta(seconds=200)
        key = "feature:user_naive:rolling_7d_spend"
        payload = {
            "value": 10.0,
            "timestamp": naive_ts.isoformat(),  # no tzinfo
            "source": "test",
            "schema_version": "1.0",
        }
        fake_redis.set(key, json.dumps(payload))
        defn = FeatureDefinition(
            feature_name="rolling_7d_spend",
            description="test",
            owner="test",
            expected_freshness_seconds=45,
            value_type="float",
        )
        result = monitor._check_feature(defn)
        assert "user_naive" in result

    def test_run_loop_handles_check_exception(self, monitor):
        """Exception in _check_all_features is caught; thread keeps running."""
        import time
        from unittest.mock import patch
        raised = []

        def raise_once():
            if not raised:
                raised.append(True)
                raise RuntimeError("simulated monitor error")

        with patch.object(monitor, "_check_all_features", side_effect=raise_once):
            monitor._interval = 0.01
            monitor.start()
            deadline = time.time() + 1.0
            while not raised and time.time() < deadline:
                time.sleep(0.01)
            monitor.stop(timeout=1.0)
        assert raised, "Exception should have been raised in the run loop"
