"""Demo script — push 1000 synthetic demand-forecasting feature events at 10 events/sec.

Demonstrates sub-60s feature freshness in a realistic demand-forecasting scenario.
Each synthetic entity represents a customer/order in a retail system.

Usage:
    python scripts/simulate_producer.py
    python scripts/simulate_producer.py --events 500 --rate 20
"""

from __future__ import annotations
from feature_store.schemas.feature_event import FeatureEvent
from feature_store.producer import FeatureProducer, ensure_topic_exists

import argparse
import logging
import random
import sys
import time
from datetime import datetime
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Feature definitions matching the registry seeds in serving.py
FEATURE_GENERATORS = {
    "rolling_7d_spend": lambda: round(random.uniform(0.0, 2000.0), 2),
    "order_count_24h": lambda: random.randint(0, 50),
    "avg_basket_size": lambda: round(random.uniform(5.0, 300.0), 2),
    "days_since_last_order": lambda: round(random.uniform(0.0, 180.0), 1),
    "preferred_category": lambda: random.choice(
        ["electronics", "groceries", "apparel", "home_garden", "sports", "beauty"]
    ),
}

ENTITY_POOL = [f"customer_{i:05d}" for i in range(1, 501)]  # 500 synthetic customers


def generate_event(entity_id: str | None = None) -> FeatureEvent:
    """Create a random feature event for a random entity and feature."""
    entity = entity_id or random.choice(ENTITY_POOL)
    feature_name, generator = random.choice(list(FEATURE_GENERATORS.items()))
    return FeatureEvent(
        entity_id=entity,
        feature_name=feature_name,
        value=generator(),
        timestamp=datetime.utcnow(),
        source="simulate_producer",
    )


def run(num_events: int = 1000, rate_per_sec: int = 10) -> None:
    """Publish `num_events` events to Kafka at `rate_per_sec` events/second."""
    interval = 1.0 / rate_per_sec

    logger.info(
        f"Starting simulation: {num_events} events at {rate_per_sec}/sec "
        f"(estimated duration: {num_events / rate_per_sec:.1f}s)"
    )

    # Ensure topic exists before producing
    ensure_topic_exists()

    start_time = time.monotonic()
    published = 0
    failed = 0

    with FeatureProducer() as producer:
        for i in range(num_events):
            event = generate_event()
            try:
                producer.publish(event)
                published += 1
            except Exception as exc:
                failed += 1
                logger.error(f"Failed to publish event {i}: {exc}")

            # Progress log every 100 events
            if (i + 1) % 100 == 0:
                elapsed = time.monotonic() - start_time
                actual_rate = published / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Progress: {i + 1}/{num_events} events "
                    f"| rate: {actual_rate:.1f}/s "
                    f"| elapsed: {elapsed:.1f}s"
                )

            time.sleep(interval)

        producer.flush()

    elapsed_total = time.monotonic() - start_time
    logger.info(
        f"Simulation complete: {published} published, {failed} failed "
        f"in {elapsed_total:.2f}s ({published / elapsed_total:.1f} events/sec)"
    )

    if published > 0:
        logger.info(
            "Features should now be available in Redis. "
            "Query: curl http://localhost:8000/features/<entity_id>"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate feature event production")
    parser.add_argument("--events", type=int, default=1000, help="Number of events to publish")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    args = parser.parse_args()
    run(num_events=args.events, rate_per_sec=args.rate)
