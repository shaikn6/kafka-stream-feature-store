"""Pydantic v2 schemas for feature events published to and consumed from Kafka."""

from datetime import datetime
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, field_validator


class FeatureEvent(BaseModel):
    """Represents a single feature observation published to Kafka."""

    entity_id: str = Field(..., description="Unique identifier for the entity")
    feature_name: str = Field(..., description="Name of the feature")
    value: Union[float, int, str, bool] = Field(..., description="Feature value")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="UTC timestamp")
    source: Optional[str] = Field(None, description="Upstream system that generated this event")
    schema_version: str = Field(default="1.0", description="Schema version")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Arbitrary metadata")

    @field_validator("feature_name")
    @classmethod
    def feature_name_snake_case(cls, v: str) -> str:
        if not v.replace("_", "").isalnum():
            raise ValueError(f"feature_name must be alphanumeric with underscores, got: {v!r}")
        if v != v.lower():
            raise ValueError(f"feature_name must be lowercase, got: {v!r}")
        return v

    @field_validator("entity_id")
    @classmethod
    def entity_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("entity_id must not be empty")
        return v

    def to_json(self) -> str:
        """Serialize to JSON string for Kafka publishing."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: Union[str, bytes]) -> "FeatureEvent":
        """Deserialize from JSON bytes or string (Kafka message value)."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return cls.model_validate_json(data)

    def redis_key(self) -> str:
        """Return the Redis key pattern: feature:{entity_id}:{feature_name}"""
        return f"feature:{self.entity_id}:{self.feature_name}"


class FeatureDefinition(BaseModel):
    """Metadata stored in the PostgreSQL feature registry."""

    feature_name: str
    description: str
    owner: str
    expected_freshness_seconds: int = Field(..., gt=0, description="Max acceptable age in seconds")
    value_type: str = Field(..., description="float | int | str | bool")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    is_active: bool = True


class FeatureResponse(BaseModel):
    """API response shape for a single feature value."""

    entity_id: str
    feature_name: str
    value: Optional[Any] = None
    timestamp: Optional[datetime] = None
    age_seconds: Optional[float] = None
    is_stale: bool
    freshness_sla_seconds: Optional[int] = None


class EntityFeaturesResponse(BaseModel):
    """API response for all features belonging to an entity."""

    entity_id: str
    features: Dict[str, FeatureResponse]
    total_features: int
    stale_features: int
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)


class HealthResponse(BaseModel):
    """Health check response from /health endpoint."""

    status: str
    stale_feature_count: int
    total_monitored_features: int
    checked_at: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[Dict[str, Any]] = None
