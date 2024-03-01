"""Unit tests for feature_store.serving (FastAPI layer).

Uses httpx's AsyncClient with the ASGI transport so no real server process
is needed. Redis is mocked via fakeredis; PostgreSQL is mocked in-memory.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional
from unittest.mock import MagicMock

import fakeredis
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from feature_store.schemas.feature_event import FeatureDefinition, FeatureResponse
from feature_store.serving import create_app


# ---------------------------------------------------------------------------
# In-memory mocks
# ---------------------------------------------------------------------------

class InMemoryRegistry:
    """Thread-safe in-memory registry for tests."""

    def __init__(self) -> None:
        self._store: Dict[str, FeatureDefinition] = {}

    def create(self, definition: FeatureDefinition) -> FeatureDefinition:
        if definition.feature_name in self._store:
            raise ValueError(f"Feature '{definition.feature_name}' already exists")
        self._store[definition.feature_name] = definition
        return definition

    def get(self, feature_name: str) -> Optional[FeatureDefinition]:
        return self._store.get(feature_name)

    def list_all(self, active_only: bool = True) -> List[FeatureDefinition]:
        return [d for d in self._store.values() if not active_only or d.is_active]

    def update(self, feature_name: str, **kwargs) -> FeatureDefinition:
        if feature_name not in self._store:
            raise KeyError(feature_name)
        defn = self._store[feature_name]
        updated = defn.copy(update=kwargs)
        self._store[feature_name] = updated
        return updated

    def delete(self, feature_name: str) -> bool:
        if feature_name not in self._store:
            return False
        self._store[feature_name].is_active = False
        return True

    def ping(self) -> bool:
        return True

    def create_tables(self) -> None:
        pass  # no-op for in-memory


class InMemoryMonitor:
    """Minimal monitor stub for testing."""

    def __init__(self) -> None:
        self._stale = 0
        self._total = 0

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def get_stale_counts(self):
        return self._stale, self._total

    def set_stale_counts(self, stale: int, total: int) -> None:
        self._stale = stale
        self._total = total


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_redis():
    server = fakeredis.FakeServer()
    return fakeredis.FakeRedis(server=server, decode_responses=True)


@pytest.fixture()
def registry():
    reg = InMemoryRegistry()
    # Seed a few feature definitions
    reg.create(FeatureDefinition(
        feature_name="rolling_7d_spend",
        description="7-day spend",
        owner="ml-platform",
        expected_freshness_seconds=45,
        value_type="float",
    ))
    reg.create(FeatureDefinition(
        feature_name="order_count_24h",
        description="24h order count",
        owner="ml-platform",
        expected_freshness_seconds=60,
        value_type="int",
    ))
    return reg


@pytest.fixture()
def monitor():
    return InMemoryMonitor()


@pytest_asyncio.fixture()
async def client(fake_redis, registry, monitor):
    """AsyncClient wired to the FastAPI app with injected dependencies."""
    app = create_app(redis_client=fake_redis, registry=registry, monitor=monitor)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def put_feature_in_redis(redis_client, entity_id: str, feature_name: str, value, age_seconds: float = 5.0) -> None:
    """Helper: write a feature payload directly to fakeredis."""
    from datetime import timedelta
    ts = datetime.now(tz=timezone.utc) - timedelta(seconds=age_seconds)
    key = f"feature:{entity_id}:{feature_name}"
    payload = {
        "value": value,
        "timestamp": ts.isoformat(),
        "source": "test",
        "schema_version": "1.0",
    }
    redis_client.set(key, json.dumps(payload))


# ---------------------------------------------------------------------------
# GET /features/{entity_id}
# ---------------------------------------------------------------------------

class TestGetEntityFeatures:
    @pytest.mark.asyncio
    async def test_returns_200_for_known_entity(self, client, fake_redis):
        put_feature_in_redis(fake_redis, "cust_001", "rolling_7d_spend", 142.5)
        response = await client.get("/features/cust_001")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_entity_id_in_response(self, client, fake_redis):
        put_feature_in_redis(fake_redis, "cust_001", "rolling_7d_spend", 99.0)
        data = (await client.get("/features/cust_001")).json()
        assert data["entity_id"] == "cust_001"

    @pytest.mark.asyncio
    async def test_feature_value_correct(self, client, fake_redis):
        put_feature_in_redis(fake_redis, "cust_002", "rolling_7d_spend", 55.5)
        data = (await client.get("/features/cust_002")).json()
        assert data["features"]["rolling_7d_spend"]["value"] == 55.5

    @pytest.mark.asyncio
    async def test_missing_feature_is_stale_true(self, client):
        """If no Redis key exists, feature is reported as stale."""
        data = (await client.get("/features/unknown_entity")).json()
        for feat in data["features"].values():
            assert feat["is_stale"] is True
            assert feat["value"] is None

    @pytest.mark.asyncio
    async def test_fresh_feature_is_stale_false(self, client, fake_redis):
        """Feature written 5 seconds ago with 45s SLA should not be stale."""
        put_feature_in_redis(fake_redis, "cust_003", "rolling_7d_spend", 10.0, age_seconds=5)
        data = (await client.get("/features/cust_003")).json()
        assert data["features"]["rolling_7d_spend"]["is_stale"] is False

    @pytest.mark.asyncio
    async def test_old_feature_is_stale_true(self, client, fake_redis):
        """Feature written 100 seconds ago with 45s SLA should be stale."""
        put_feature_in_redis(fake_redis, "cust_004", "rolling_7d_spend", 10.0, age_seconds=100)
        data = (await client.get("/features/cust_004")).json()
        assert data["features"]["rolling_7d_spend"]["is_stale"] is True

    @pytest.mark.asyncio
    async def test_total_features_count(self, client, fake_redis):
        """total_features should equal number of registered features."""
        data = (await client.get("/features/cust_005")).json()
        assert data["total_features"] == 2  # two seeded features

    @pytest.mark.asyncio
    async def test_age_seconds_is_approximate(self, client, fake_redis):
        put_feature_in_redis(fake_redis, "cust_006", "rolling_7d_spend", 1.0, age_seconds=10)
        data = (await client.get("/features/cust_006")).json()
        age = data["features"]["rolling_7d_spend"]["age_seconds"]
        assert 9 <= age <= 12  # allow 2s test-execution jitter


# ---------------------------------------------------------------------------
# GET /features/{entity_id}/{feature_name}
# ---------------------------------------------------------------------------

class TestGetSingleFeature:
    @pytest.mark.asyncio
    async def test_returns_correct_value(self, client, fake_redis):
        put_feature_in_redis(fake_redis, "cust_010", "order_count_24h", 7)
        data = (await client.get("/features/cust_010/order_count_24h")).json()
        assert data["value"] == 7

    @pytest.mark.asyncio
    async def test_missing_returns_stale(self, client):
        data = (await client.get("/features/nobody/rolling_7d_spend")).json()
        assert data["is_stale"] is True
        assert data["value"] is None

    @pytest.mark.asyncio
    async def test_freshness_sla_populated(self, client, fake_redis):
        put_feature_in_redis(fake_redis, "cust_011", "order_count_24h", 3)
        data = (await client.get("/features/cust_011/order_count_24h")).json()
        assert data["freshness_sla_seconds"] == 60


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    @pytest.mark.asyncio
    async def test_healthy_when_no_stale(self, client, monitor):
        monitor.set_stale_counts(0, 10)
        data = (await client.get("/health")).json()
        assert data["status"] == "healthy"
        assert data["stale_feature_count"] == 0

    @pytest.mark.asyncio
    async def test_degraded_when_stale(self, client, monitor):
        monitor.set_stale_counts(3, 10)
        data = (await client.get("/health")).json()
        assert data["status"] == "degraded"
        assert data["stale_feature_count"] == 3

    @pytest.mark.asyncio
    async def test_health_returns_200_always(self, client, monitor):
        monitor.set_stale_counts(99, 100)
        response = await client.get("/health")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# GET /registry
# ---------------------------------------------------------------------------

class TestRegistry:
    @pytest.mark.asyncio
    async def test_returns_list(self, client):
        data = (await client.get("/registry")).json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_seeded_features_present(self, client):
        data = (await client.get("/registry")).json()
        names = {(d["feature_name"] if isinstance(d, dict) else d) for d in data}
        assert "rolling_7d_spend" in names
        assert "order_count_24h" in names

    @pytest.mark.asyncio
    async def test_post_registry_creates_feature(self, client):
        payload = {
            "feature_name": "new_metric",
            "description": "A brand new metric",
            "owner": "data-team",
            "expected_freshness_seconds": 30,
            "value_type": "float",
        }
        response = await client.post("/registry", json=payload)
        assert response.status_code == 201
        assert response.json()["feature_name"] == "new_metric"

    @pytest.mark.asyncio
    async def test_post_registry_duplicate_returns_409(self, client):
        payload = {
            "feature_name": "rolling_7d_spend",
            "description": "duplicate",
            "owner": "x",
            "expected_freshness_seconds": 60,
            "value_type": "float",
        }
        response = await client.post("/registry", json=payload)
        assert response.status_code == 409
