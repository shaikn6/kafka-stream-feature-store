"""Tests for stream/windowed_agg.py — windowed aggregation engine.

All tests run without Kafka, Spark, or DuckDB.
"""

from __future__ import annotations

import math
import time

import pytest

from stream.windowed_agg import (
    Event,
    SlidingWindowEngine,
    TumblingWindowEngine,
    WindowBucket,
    WindowEngine,
    run_benchmark,
    synthetic_event_stream,
)


# ---------------------------------------------------------------------------
# WindowBucket tests
# ---------------------------------------------------------------------------

class TestWindowBucket:
    def test_empty_bucket_avg_is_zero(self):
        b = WindowBucket(window_start=0, window_end=60)
        assert b.avg_amount == 0.0

    def test_add_single_event(self):
        b = WindowBucket(window_start=0, window_end=60)
        ev = Event(ts=30.0, user_id="u1", amount=100.0)
        b.add(ev)
        assert b.count == 1
        assert b.total_amount == 100.0
        assert b.min_amount == 100.0
        assert b.max_amount == 100.0
        assert b.unique_users == 1

    def test_add_multiple_events_same_user(self):
        b = WindowBucket(window_start=0, window_end=60)
        for amt in [10.0, 20.0, 30.0]:
            b.add(Event(ts=10.0, user_id="u1", amount=amt))
        assert b.count == 3
        assert b.avg_amount == pytest.approx(20.0)
        assert b.unique_users == 1   # same user

    def test_add_multiple_users(self):
        b = WindowBucket(window_start=0, window_end=60)
        b.add(Event(ts=1.0, user_id="a", amount=50.0))
        b.add(Event(ts=2.0, user_id="b", amount=50.0))
        assert b.unique_users == 2

    def test_to_dict_keys(self):
        b = WindowBucket(window_start=100, window_end=160)
        b.add(Event(ts=120.0, user_id="u1", amount=42.0))
        d = b.to_dict()
        for key in ("window_start", "window_end", "count", "total_amount",
                    "avg_amount", "min_amount", "max_amount", "unique_users"):
            assert key in d

    def test_min_max_tracking(self):
        b = WindowBucket(window_start=0, window_end=60)
        amounts = [5.0, 99.0, 50.0, 1.0, 200.0]
        for a in amounts:
            b.add(Event(ts=10.0, user_id="u", amount=a))
        assert b.min_amount == 1.0
        assert b.max_amount == 200.0


# ---------------------------------------------------------------------------
# TumblingWindowEngine tests
# ---------------------------------------------------------------------------

class TestTumblingWindowEngine:
    def test_single_event_single_bucket(self):
        eng = TumblingWindowEngine(size_seconds=60)
        eng.ingest(Event(ts=100.0, user_id="u1", amount=50.0))
        results = eng.flush()
        assert len(results) == 1
        assert results[0]["count"] == 1

    def test_events_in_same_window_share_bucket(self):
        eng = TumblingWindowEngine(size_seconds=60)
        for i in range(5):
            eng.ingest(Event(ts=float(i * 10), user_id="u1", amount=10.0))  # all in [0,60)
        assert eng.bucket_count() == 1
        assert eng.flush()[0]["count"] == 5

    def test_events_in_adjacent_windows(self):
        eng = TumblingWindowEngine(size_seconds=60)
        eng.ingest(Event(ts=30.0,  user_id="u", amount=10.0))   # window 0
        eng.ingest(Event(ts=90.0,  user_id="u", amount=20.0))   # window 60
        eng.ingest(Event(ts=150.0, user_id="u", amount=30.0))   # window 120
        assert eng.bucket_count() == 3

    def test_window_boundaries_exclusive_right(self):
        eng = TumblingWindowEngine(size_seconds=60)
        eng.ingest(Event(ts=59.9, user_id="u", amount=1.0))
        eng.ingest(Event(ts=60.0, user_id="u", amount=2.0))
        assert eng.bucket_count() == 2

    def test_flush_sorted_by_window_start(self):
        eng = TumblingWindowEngine(size_seconds=60)
        for ts in [180.0, 60.0, 0.0, 240.0]:
            eng.ingest(Event(ts=ts, user_id="u", amount=1.0))
        results = eng.flush()
        starts = [r["window_start"] for r in results]
        assert starts == sorted(starts)

    def test_five_minute_windows(self):
        eng = TumblingWindowEngine(size_seconds=300)
        # 1 event per 5-minute window over 1 hour = 12 windows
        for i in range(12):
            eng.ingest(Event(ts=float(i * 300 + 1), user_id="u", amount=1.0))
        assert eng.bucket_count() == 12

    def test_one_hour_tumbling(self):
        eng = TumblingWindowEngine(size_seconds=3600)
        # All events within first hour
        for i in range(100):
            eng.ingest(Event(ts=float(i * 30), user_id="u", amount=1.0))
        assert eng.bucket_count() == 1
        assert eng.flush()[0]["count"] == 100


# ---------------------------------------------------------------------------
# SlidingWindowEngine tests
# ---------------------------------------------------------------------------

class TestSlidingWindowEngine:
    def test_empty_query(self):
        eng = SlidingWindowEngine(window_seconds=60, step_seconds=10)
        result = eng.query()
        assert result["count"] == 0

    def test_single_event_in_window(self):
        eng = SlidingWindowEngine(window_seconds=60, step_seconds=10)
        eng.ingest(Event(ts=1000.0, user_id="u", amount=42.0))
        result = eng.query(at_ts=1030.0)
        assert result["count"] == 1
        assert result["total_amount"] == pytest.approx(42.0)

    def test_event_outside_window_excluded(self):
        eng = SlidingWindowEngine(window_seconds=60, step_seconds=10)
        eng.ingest(Event(ts=900.0, user_id="u", amount=99.0))   # outside
        eng.ingest(Event(ts=1000.0, user_id="u", amount=1.0))   # inside
        result = eng.query(at_ts=1030.0)
        assert result["count"] == 1
        assert result["total_amount"] == pytest.approx(1.0)

    def test_buffer_pruning_bounds_memory(self):
        eng = SlidingWindowEngine(window_seconds=60, step_seconds=10, max_buffer_size=100)
        for i in range(500):
            eng.ingest(Event(ts=float(i), user_id="u", amount=1.0))
        assert eng.buffer_size() <= 100

    def test_compute_all_windows_returns_list(self):
        eng = SlidingWindowEngine(window_seconds=300, step_seconds=60)
        for i in range(100):
            eng.ingest(Event(ts=float(i * 10), user_id="u", amount=1.0))
        windows = eng.compute_all_windows()
        assert isinstance(windows, list)
        assert len(windows) > 0

    def test_empty_buffer_compute_all_windows(self):
        eng = SlidingWindowEngine()
        assert eng.compute_all_windows() == []


# ---------------------------------------------------------------------------
# WindowEngine (multi-resolution) tests
# ---------------------------------------------------------------------------

class TestWindowEngine:
    def test_ingested_counter(self):
        eng = WindowEngine()
        for ev in synthetic_event_stream(n=50, seed=1):
            eng.ingest(ev)
        assert eng.total_ingested == 50

    def test_flush_all_returns_all_window_types(self):
        eng = WindowEngine()
        for ev in synthetic_event_stream(n=100, seed=2):
            eng.ingest(ev)
        results = eng.flush_all()
        for key in ("1min", "5min", "1hr", "sliding"):
            assert key in results

    def test_1min_has_more_buckets_than_1hr(self):
        eng = WindowEngine()
        for ev in synthetic_event_stream(n=1000, duration_seconds=7200.0, seed=3):
            eng.ingest(ev)
        results = eng.flush_all()
        assert len(results["1min"]) >= len(results["1hr"])

    def test_all_events_accounted_in_1min_windows(self):
        eng = WindowEngine()
        events = list(synthetic_event_stream(n=500, seed=4))
        for ev in events:
            eng.ingest(ev)
        total_in_windows = sum(r["count"] for r in eng.flush_all()["1min"])
        assert total_in_windows == 500


# ---------------------------------------------------------------------------
# Synthetic event stream tests
# ---------------------------------------------------------------------------

class TestSyntheticEventStream:
    def test_correct_count(self):
        events = list(synthetic_event_stream(n=200, seed=0))
        assert len(events) == 200

    def test_events_sorted_by_ts(self):
        events = list(synthetic_event_stream(n=500, seed=1))
        for i in range(1, len(events)):
            assert events[i].ts >= events[i - 1].ts

    def test_user_ids_bounded(self):
        events = list(synthetic_event_stream(n=1000, n_users=10, seed=2))
        user_ids = {ev.user_id for ev in events}
        assert len(user_ids) <= 10

    def test_amounts_positive(self):
        events = list(synthetic_event_stream(n=100, seed=3))
        assert all(ev.amount > 0 for ev in events)

    def test_reproducible_with_same_seed(self):
        a = list(synthetic_event_stream(n=50, seed=99))
        b = list(synthetic_event_stream(n=50, seed=99))
        assert [ev.ts for ev in a] == [ev.ts for ev in b]


# ---------------------------------------------------------------------------
# Throughput benchmark (sanity only — not timing-sensitive in CI)
# ---------------------------------------------------------------------------

class TestBenchmark:
    def test_benchmark_returns_dict(self):
        result = run_benchmark(n_events=1_000)   # small for CI speed
        assert isinstance(result, dict)
        assert "events_per_second" in result
        assert "total_events" in result
        assert result["total_events"] == 1_000

    def test_benchmark_window_counts_positive(self):
        result = run_benchmark(n_events=500)
        for wname, cnt in result["window_counts"].items():
            assert cnt >= 0, f"Negative window count for {wname}"
