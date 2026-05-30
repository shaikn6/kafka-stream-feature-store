"""SLA monitor — detects features that have exceeded their freshness window.

Runs as a background thread. Periodically:
  1. Fetches all registered feature definitions
  2. Scans Redis for known entity keys matching each feature
  3. Compares last-seen timestamp against expected_freshness_seconds
  4. Increments stale counter and emits a structured log alert per violation

Exposes get_stale_counts() for the /health endpoint.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import redis

from feature_store.registry import FeatureRegistry
from feature_store.schemas.feature_event import FeatureDefinition

logger = logging.getLogger(__name__)

MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "15"))
REDIS_SCAN_COUNT = int(os.getenv("REDIS_SCAN_COUNT", "1000"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))


class FeatureMonitor:
    """Background SLA monitor for the streaming feature store.

    Tracks stale feature count in-memory for the /health endpoint.

    Usage::

        monitor = FeatureMonitor(registry=registry, redis_client=r)
        monitor.start()    # starts daemon thread
        ...
        stale, total = monitor.get_stale_counts()
        monitor.stop()
    """

    def __init__(
        self,
        registry: Optional[FeatureRegistry] = None,
        redis_client: Optional[redis.Redis] = None,
        interval_seconds: int = MONITOR_INTERVAL_SECONDS,
    ) -> None:
        self._registry = registry
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # Mutable state — protected by _lock
        self._stale_count: int = 0
        self._total_checked: int = 0
        self._last_run: Optional[datetime] = None
        self._stale_details: Dict[str, List[str]] = {}  # feature_name -> [entity_ids]

        self._redis = redis_client or redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

        # Total stale events observed since process start (for Prometheus-style gauge)
        self.total_sla_violations: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the monitoring loop in a background daemon thread."""
        self._thread = threading.Thread(target=self._run, name="feature-monitor", daemon=True)
        self._thread.start()
        logger.info("FeatureMonitor started", extra={"interval_seconds": self._interval})

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the monitor to stop and wait for thread to finish."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("FeatureMonitor stopped")

    # ------------------------------------------------------------------
    # Public state accessors (thread-safe)
    # ------------------------------------------------------------------

    def get_stale_counts(self) -> Tuple[int, int]:
        """Return (stale_count, total_monitored) from the most recent check."""
        with self._lock:
            return self._stale_count, self._total_checked

    def get_stale_details(self) -> Dict[str, List[str]]:
        """Return mapping of feature_name -> list of stale entity_ids."""
        with self._lock:
            return dict(self._stale_details)

    def get_last_run(self) -> Optional[datetime]:
        """Return UTC timestamp of most recent monitoring cycle."""
        with self._lock:
            return self._last_run

    # ------------------------------------------------------------------
    # Core monitoring loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._check_all_features()
            except Exception as exc:
                logger.error("Monitor check failed", extra={"error": str(exc)})
            self._stop_event.wait(timeout=self._interval)

    def _check_all_features(self) -> None:
        """Scan Redis for all known feature keys and evaluate freshness."""
        if not self._registry:
            return

        definitions = self._registry.list_all()
        if not definitions:
            return

        stale_count = 0
        total_checked = 0
        stale_details: Dict[str, List[str]] = {}

        for defn in definitions:
            stale_entities = self._check_feature(defn)
            feature_stale = len(stale_entities)
            total_keys = self._count_feature_keys(defn.feature_name)

            total_checked += total_keys
            stale_count += feature_stale

            if stale_entities:
                stale_details[defn.feature_name] = stale_entities
                logger.warning(
                    "SLA violation detected",
                    extra={
                        "feature": defn.feature_name,
                        "stale_entities": feature_stale,
                        "sla_seconds": defn.expected_freshness_seconds,
                    },
                )
                self.total_sla_violations += feature_stale

        with self._lock:
            self._stale_count = stale_count
            self._total_checked = total_checked
            self._stale_details = stale_details
            self._last_run = datetime.now(tz=timezone.utc)

        logger.info(
            "Monitor cycle complete",
            extra={
                "total_checked": total_checked,
                "stale_count": stale_count,
                "features_monitored": len(definitions),
            },
        )

    def _check_feature(self, defn: FeatureDefinition) -> List[str]:
        """Return list of entity_ids where this feature is stale."""
        stale_entities: List[str] = []
        pattern = f"feature:*:{defn.feature_name}"
        now = datetime.now(tz=timezone.utc)

        for key in self._scan_keys(pattern):
            raw = self._redis.get(key)
            if raw is None:
                # Key expired between SCAN and GET — not stale, just gone
                continue

            try:
                payload = json.loads(raw)
                ts = datetime.fromisoformat(payload["timestamp"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_seconds = (now - ts).total_seconds()
                if age_seconds > defn.expected_freshness_seconds:
                    entity_id = key.split(":")[1]
                    stale_entities.append(entity_id)
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                logger.debug("Could not parse feature key", extra={"key": key, "error": str(exc)})

        return stale_entities

    def _count_feature_keys(self, feature_name: str) -> int:
        """Count how many Redis keys exist for a given feature name."""
        pattern = f"feature:*:{feature_name}"
        return sum(1 for _ in self._scan_keys(pattern))

    def _scan_keys(self, pattern: str):  # type: ignore[no-untyped-def]
        """Generator that iterates over Redis keys matching a pattern using SCAN."""
        cursor = 0
        while True:
            cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=REDIS_SCAN_COUNT)
            yield from keys
            if cursor == 0:
                break
