"""FastAPI serving layer — expose feature values with freshness metadata."""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional

import redis
from fastapi import FastAPI, HTTPException, Path, status

from feature_store.monitor import FeatureMonitor
from feature_store.registry import FeatureRegistry
from feature_store.schemas.feature_event import (
    EntityFeaturesResponse,
    FeatureDefinition,
    FeatureResponse,
    HealthResponse,
)

# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------

# Allow alphanumeric, hyphens, underscores, and dots in entity IDs.
# Colons are explicitly excluded to prevent Redis key structure injection.
_ENTITY_ID_RE = re.compile(r"^[a-zA-Z0-9_\-\.]{1,128}$")

# Feature names must already be validated as snake_case by FeatureEvent, but
# we enforce the same pattern here for the serving layer's direct lookup path.
_FEATURE_NAME_RE = re.compile(r"^[a-z0-9_]{1,64}$")


def _validate_entity_id(entity_id: str) -> str:
    """Raise 400 if entity_id contains characters that would pollute the Redis key."""
    if not _ENTITY_ID_RE.match(entity_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "entity_id must be 1–128 characters and contain only "
                "alphanumerics, hyphens, underscores, or dots."
            ),
        )
    return entity_id


def _validate_feature_name(feature_name: str) -> str:
    """Raise 400 if feature_name contains characters outside the allowed set."""
    if not _FEATURE_NAME_RE.match(feature_name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "feature_name must be 1–64 lowercase alphanumeric characters "
                "with underscores only."
            ),
        )
    return feature_name

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://featurestore:featurestore@localhost:5432/featurestore",
)


def create_app(
    redis_client: Optional[redis.Redis] = None,
    registry: Optional[FeatureRegistry] = None,
    monitor: Optional[FeatureMonitor] = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""

    # Mutable container so the lifespan closure can update the references
    deps: Dict[str, object] = {
        "redis": redis_client,
        "registry": registry,
        "monitor": monitor,
    }

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
        # Startup
        if deps["redis"] is None:
            deps["redis"] = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
            )
        if deps["registry"] is None:
            reg = FeatureRegistry(db_url=DATABASE_URL)
            reg.create_tables()
            _seed_default_features(reg)
            deps["registry"] = reg
        if deps["monitor"] is None:
            mon = FeatureMonitor(
                registry=deps["registry"],  # type: ignore[arg-type]
                redis_client=deps["redis"],  # type: ignore[arg-type]
            )
            mon.start()
            deps["monitor"] = mon
        logger.info("Feature store API started")
        yield
        # Shutdown
        if deps["monitor"]:
            deps["monitor"].stop()  # type: ignore[union-attr]
        logger.info("Feature store API shut down")

    app = FastAPI(
        title="Kafka Stream Feature Store",
        description="Real-time feature serving with sub-60s freshness guarantees",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Helper accessors (use at request time, after startup)
    def _redis() -> redis.Redis:
        return deps["redis"]  # type: ignore[return-value]

    def _registry() -> FeatureRegistry:
        return deps["registry"]  # type: ignore[return-value]

    def _monitor() -> Optional[FeatureMonitor]:
        return deps["monitor"]  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.get("/features/{entity_id}", response_model=EntityFeaturesResponse)
    async def get_entity_features(entity_id: str) -> EntityFeaturesResponse:
        _validate_entity_id(entity_id)
        definitions = _registry().list_all()
        features: Dict[str, FeatureResponse] = {}

        for defn in definitions:
            key = f"feature:{entity_id}:{defn.feature_name}"
            raw = _redis().get(key)

            if raw is None:
                features[defn.feature_name] = FeatureResponse(
                    entity_id=entity_id,
                    feature_name=defn.feature_name,
                    value=None,
                    timestamp=None,
                    age_seconds=None,
                    is_stale=True,
                    freshness_sla_seconds=defn.expected_freshness_seconds,
                )
                continue

            payload = json.loads(raw)
            ts = datetime.fromisoformat(payload["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            now = datetime.now(tz=timezone.utc)
            age_seconds = (now - ts).total_seconds()
            is_stale = age_seconds > defn.expected_freshness_seconds

            features[defn.feature_name] = FeatureResponse(
                entity_id=entity_id,
                feature_name=defn.feature_name,
                value=payload["value"],
                timestamp=ts,
                age_seconds=round(age_seconds, 2),
                is_stale=is_stale,
                freshness_sla_seconds=defn.expected_freshness_seconds,
            )

        stale_count = sum(1 for f in features.values() if f.is_stale)
        return EntityFeaturesResponse(
            entity_id=entity_id,
            features=features,
            total_features=len(features),
            stale_features=stale_count,
        )

    @app.get("/features/{entity_id}/{feature_name}", response_model=FeatureResponse)
    async def get_single_feature(entity_id: str, feature_name: str) -> FeatureResponse:
        _validate_entity_id(entity_id)
        _validate_feature_name(feature_name)
        defn = _registry().get(feature_name)
        freshness_sla = defn.expected_freshness_seconds if defn else None

        key = f"feature:{entity_id}:{feature_name}"
        raw = _redis().get(key)

        if raw is None:
            return FeatureResponse(
                entity_id=entity_id,
                feature_name=feature_name,
                value=None,
                timestamp=None,
                age_seconds=None,
                is_stale=True,
                freshness_sla_seconds=freshness_sla,
            )

        payload = json.loads(raw)
        ts = datetime.fromisoformat(payload["timestamp"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        age_seconds = (now - ts).total_seconds()
        is_stale = freshness_sla is not None and age_seconds > freshness_sla

        return FeatureResponse(
            entity_id=entity_id,
            feature_name=feature_name,
            value=payload["value"],
            timestamp=ts,
            age_seconds=round(age_seconds, 2),
            is_stale=is_stale,
            freshness_sla_seconds=freshness_sla,
        )

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        mon = _monitor()
        stale_count, total = mon.get_stale_counts() if mon else (0, 0)
        status_str = "degraded" if stale_count > 0 else "healthy"
        return HealthResponse(
            status=status_str,
            stale_feature_count=stale_count,
            total_monitored_features=total,
        )

    @app.get("/registry", response_model=List[FeatureDefinition])
    async def list_registry() -> List[FeatureDefinition]:
        return _registry().list_all()

    @app.post("/registry", response_model=FeatureDefinition, status_code=status.HTTP_201_CREATED)
    async def register_feature(definition: FeatureDefinition) -> FeatureDefinition:
        try:
            return _registry().create(definition)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))

    return app


def _seed_default_features(registry: FeatureRegistry) -> None:
    defaults = [
        FeatureDefinition(
            feature_name="rolling_7d_spend",
            description="Sum of customer spend over past 7 days",
            owner="ml-platform",
            expected_freshness_seconds=45,
            value_type="float",
        ),
        FeatureDefinition(
            feature_name="order_count_24h",
            description="Number of orders placed in the past 24 hours",
            owner="ml-platform",
            expected_freshness_seconds=60,
            value_type="int",
        ),
        FeatureDefinition(
            feature_name="avg_basket_size",
            description="Average basket size over last 30 orders",
            owner="ml-platform",
            expected_freshness_seconds=90,
            value_type="float",
        ),
        FeatureDefinition(
            feature_name="days_since_last_order",
            description="Days elapsed since most recent order",
            owner="ml-platform",
            expected_freshness_seconds=120,
            value_type="float",
        ),
        FeatureDefinition(
            feature_name="preferred_category",
            description="Most frequently purchased product category",
            owner="ml-platform",
            expected_freshness_seconds=300,
            value_type="str",
        ),
    ]
    for defn in defaults:
        try:
            registry.create(defn)
        except ValueError:
            pass


# ASGI entry point
app = create_app()
