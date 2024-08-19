"""Windowed aggregation engine — V2.

Pure-Python implementation of tumbling and sliding window aggregations over a
synthetic event stream.  No Flink, Spark Streaming, or Kafka required.

Window types
------------
- **Tumbling**: non-overlapping fixed-size windows (1 min, 5 min, 1 hr)
- **Sliding**: overlapping windows with a configurable step size

Performance target: ≥ 100 K events/sec on a single core (no threads, no async).

Usage::

    engine = WindowEngine()
    for event in event_stream:
        engine.ingest(event)
    results = engine.flush_all()

Or via CLI::

    python stream/windowed_agg.py --events 500000
"""

from __future__ import annotations

import collections
import logging
import math
import random
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Event:
    """A single timestamped event in the stream.

    ``ts`` is a Unix timestamp (seconds, float precision).
    ``user_id`` and ``amount`` are the payload fields used for aggregation.
    All other fields are ignored by the aggregation engine.
    """

    ts: float           # sort key — used by heapq
    user_id: str = field(compare=False)
    amount: float = field(compare=False, default=0.0)
    event_type: str = field(compare=False, default="txn")


# ---------------------------------------------------------------------------
# Window bucket
# ---------------------------------------------------------------------------

@dataclass
class WindowBucket:
    """Accumulates statistics for events within a single window slot."""

    window_start: float
    window_end: float
    count: int = 0
    total_amount: float = 0.0
    min_amount: float = float("inf")
    max_amount: float = float("-inf")
    user_ids: set = field(default_factory=set)

    def add(self, event: Event) -> None:
        self.count += 1
        self.total_amount += event.amount
        self.min_amount = min(self.min_amount, event.amount)
        self.max_amount = max(self.max_amount, event.amount)
        self.user_ids.add(event.user_id)

    @property
    def avg_amount(self) -> float:
        return self.total_amount / self.count if self.count else 0.0

    @property
    def unique_users(self) -> int:
        return len(self.user_ids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "count": self.count,
            "total_amount": round(self.total_amount, 2),
            "avg_amount": round(self.avg_amount, 2),
            "min_amount": round(self.min_amount, 2) if self.min_amount != float("inf") else 0.0,
            "max_amount": round(self.max_amount, 2) if self.max_amount != float("-inf") else 0.0,
            "unique_users": self.unique_users,
        }


# ---------------------------------------------------------------------------
# Tumbling window engine
# ---------------------------------------------------------------------------

class TumblingWindowEngine:
    """Non-overlapping fixed-size window aggregator.

    Events are bucketed by ``floor(event.ts / size_seconds) * size_seconds``.
    This means any event with ``ts`` in [W, W + size)`` falls into bucket W.

    Parameters
    ----------
    size_seconds:
        Window duration in seconds (60, 300, 3600 etc.).
    """

    def __init__(self, size_seconds: float) -> None:
        self._size = size_seconds
        self._buckets: dict[float, WindowBucket] = {}

    def ingest(self, event: Event) -> None:
        """Place event into the appropriate tumbling bucket."""
        w_start = math.floor(event.ts / self._size) * self._size
        if w_start not in self._buckets:
            self._buckets[w_start] = WindowBucket(
                window_start=w_start,
                window_end=w_start + self._size,
            )
        self._buckets[w_start].add(event)

    def flush(self) -> list[dict[str, Any]]:
        """Return all closed window results sorted by window_start."""
        results = [b.to_dict() for b in self._buckets.values()]
        results.sort(key=lambda r: r["window_start"])
        return results

    def bucket_count(self) -> int:
        return len(self._buckets)


# ---------------------------------------------------------------------------
# Sliding window engine
# ---------------------------------------------------------------------------

class SlidingWindowEngine:
    """Overlapping sliding window aggregator implemented with a deque.

    Each event is stored in a deque ordered by ``ts``.  When ``query()`` is
    called the engine slides a window of ``window_seconds`` ending at ``ts``
    over the buffer and returns aggregated stats.

    The deque is pruned whenever the oldest event falls outside
    ``window_seconds + step_seconds`` of the most recent event, bounding
    memory usage to approximately ``window_size / step_size`` active windows.

    Parameters
    ----------
    window_seconds:
        Size of the sliding window.
    step_seconds:
        How often a new window starts.
    max_buffer_size:
        Hard cap on the deque length to bound memory.
    """

    def __init__(
        self,
        window_seconds: float = 300.0,
        step_seconds: float = 60.0,
        max_buffer_size: int = 500_000,
    ) -> None:
        self._window = window_seconds
        self._step = step_seconds
        self._max_buf = max_buffer_size
        self._buffer: collections.deque[Event] = collections.deque()
        self._last_ts: float = 0.0

    def ingest(self, event: Event) -> None:
        """Append an event to the sliding buffer."""
        self._buffer.append(event)
        self._last_ts = max(self._last_ts, event.ts)

        # Prune events older than window + step to bound memory
        cutoff = self._last_ts - (self._window + self._step)
        while self._buffer and self._buffer[0].ts < cutoff:
            self._buffer.popleft()

        # Hard cap
        while len(self._buffer) > self._max_buf:
            self._buffer.popleft()

    def query(self, at_ts: Optional[float] = None) -> dict[str, Any]:
        """Return aggregated stats for the window ending at ``at_ts``.

        If ``at_ts`` is None, uses the most recent ingested timestamp.
        """
        end = at_ts if at_ts is not None else self._last_ts
        start = end - self._window

        bucket = WindowBucket(window_start=start, window_end=end)
        for ev in self._buffer:
            if start <= ev.ts <= end:
                bucket.add(ev)

        return bucket.to_dict()

    def compute_all_windows(self) -> list[dict[str, Any]]:
        """Compute all non-overlapping step-aligned windows over the buffer."""
        if not self._buffer:
            return []

        first_ts = self._buffer[0].ts
        last_ts = self._buffer[-1].ts

        # Align step to first event
        windows = []
        cursor = math.ceil(first_ts / self._step) * self._step
        while cursor + self._window <= last_ts + self._step:
            windows.append(self.query(at_ts=cursor + self._window))
            cursor += self._step

        return windows

    def buffer_size(self) -> int:
        return len(self._buffer)


# ---------------------------------------------------------------------------
# Multi-resolution engine
# ---------------------------------------------------------------------------

_TUMBLING_CONFIGS = {
    "1min": 60,
    "5min": 300,
    "1hr": 3600,
}


class WindowEngine:
    """Orchestrates multiple window types in a single pass.

    Maintains one TumblingWindowEngine for each of 1min / 5min / 1hr, plus
    one SlidingWindowEngine with configurable parameters.

    Usage::

        engine = WindowEngine()
        for ev in events:
            engine.ingest(ev)
        results = engine.flush_all()
    """

    def __init__(
        self,
        sliding_window_seconds: float = 300.0,
        sliding_step_seconds: float = 60.0,
    ) -> None:
        self._tumblers = {
            name: TumblingWindowEngine(size)
            for name, size in _TUMBLING_CONFIGS.items()
        }
        self._slider = SlidingWindowEngine(
            window_seconds=sliding_window_seconds,
            step_seconds=sliding_step_seconds,
        )
        self._ingested: int = 0

    def ingest(self, event: Event) -> None:
        """Route event to all window engines."""
        for t in self._tumblers.values():
            t.ingest(event)
        self._slider.ingest(event)
        self._ingested += 1

    def flush_all(self) -> dict[str, list[dict[str, Any]]]:
        """Return all window results keyed by window type."""
        return {
            **{name: t.flush() for name, t in self._tumblers.items()},
            "sliding": self._slider.compute_all_windows(),
        }

    @property
    def total_ingested(self) -> int:
        return self._ingested


# ---------------------------------------------------------------------------
# Synthetic event stream generator
# ---------------------------------------------------------------------------

def synthetic_event_stream(
    n: int = 100_000,
    n_users: int = 1_000,
    start_ts: Optional[float] = None,
    duration_seconds: float = 3600.0,
    seed: int = 42,
) -> Iterator[Event]:
    """Yield ``n`` synthetic events spread over ``duration_seconds``.

    Events are yielded in timestamp order (sorted on generation).
    """
    rng = random.Random(seed)
    base_ts = start_ts or 1_700_000_000.0  # arbitrary fixed epoch for reproducibility

    events = []
    for i in range(n):
        ts = base_ts + rng.uniform(0, duration_seconds)
        user_id = f"user_{rng.randint(0, n_users - 1):05d}"
        amount = round(rng.uniform(1.0, 500.0), 2)
        events.append(Event(ts=ts, user_id=user_id, amount=amount))

    events.sort()   # ascending by ts (heapq order)
    yield from events


# ---------------------------------------------------------------------------
# Throughput benchmark
# ---------------------------------------------------------------------------

def run_benchmark(n_events: int = 500_000) -> dict[str, Any]:
    """Ingest ``n_events`` events and measure throughput.

    Returns
    -------
    dict with ``events_per_second``, ``total_events``, ``elapsed_seconds``.
    """
    logger.info("Generating %d synthetic events …", n_events)
    events = list(synthetic_event_stream(n=n_events))

    engine = WindowEngine()

    logger.info("Starting ingestion benchmark …")
    t0 = time.perf_counter()
    for ev in events:
        engine.ingest(ev)
    elapsed = time.perf_counter() - t0

    eps = n_events / elapsed if elapsed > 0 else float("inf")
    results = engine.flush_all()

    summary = {
        "total_events": n_events,
        "elapsed_seconds": round(elapsed, 4),
        "events_per_second": round(eps, 0),
        "target_100k_eps": eps >= 100_000,
        "window_counts": {k: len(v) for k, v in results.items()},
    }

    print("\n=== Windowed Aggregation Benchmark ===")
    print(f"Events ingested : {n_events:,}")
    print(f"Elapsed         : {elapsed:.4f}s")
    print(f"Throughput      : {eps:,.0f} events/sec")
    target_status = "PASS (>= 100K/s)" if eps >= 100_000 else f"WARN ({eps:,.0f}/s)"
    print(f"Target 100K/s   : {target_status}")
    print("Window bucket counts:")
    for wname, cnt in summary["window_counts"].items():
        print(f"  {wname:<10} {cnt:>6} windows")

    return summary


# ---------------------------------------------------------------------------
# Optional type alias (avoids circular import issues in Python < 3.10)
# ---------------------------------------------------------------------------

Optional = __import__("typing").Optional  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="V2 windowed aggregation engine")
    parser.add_argument("--events", type=int, default=500_000)
    parser.add_argument("--users", type=int, default=1_000)
    args = parser.parse_args()

    run_benchmark(n_events=args.events)
