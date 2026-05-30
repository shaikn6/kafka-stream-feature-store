"""Feature registry — PostgreSQL-backed catalogue of feature definitions."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import List, Optional

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, create_engine, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from feature_store.schemas.feature_event import FeatureDefinition

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://featurestore:featurestore@localhost:5432/featurestore",
)


class Base(DeclarativeBase):
    pass


class FeatureDefinitionModel(Base):
    __tablename__ = "feature_definitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    owner = Column(String(255), nullable=False)
    expected_freshness_seconds = Column(Integer, nullable=False, default=60)
    value_type = Column(String(50), nullable=False, default="float")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class FeatureRegistry:
    """CRUD interface for feature definitions stored in PostgreSQL."""

    def __init__(self, db_url: str = DATABASE_URL, echo: bool = False) -> None:
        self._engine = create_engine(db_url, echo=echo, pool_pre_ping=True)
        self._SessionLocal = sessionmaker(bind=self._engine, expire_on_commit=False)
        logger.info("FeatureRegistry connected", extra={"db": db_url.split("@")[-1]})

    def create_tables(self) -> None:
        Base.metadata.create_all(bind=self._engine)
        logger.info("Registry tables ensured")

    def drop_tables(self) -> None:
        Base.metadata.drop_all(bind=self._engine)

    def create(self, definition: FeatureDefinition) -> FeatureDefinition:
        with self._SessionLocal() as session:
            model = FeatureDefinitionModel(
                feature_name=definition.feature_name,
                description=definition.description,
                owner=definition.owner,
                expected_freshness_seconds=definition.expected_freshness_seconds,
                value_type=definition.value_type,
                is_active=definition.is_active,
            )
            session.add(model)
            try:
                session.commit()
                session.refresh(model)
            except IntegrityError:
                session.rollback()
                raise ValueError(f"Feature '{definition.feature_name}' already exists in registry")
            return self._to_pydantic(model)

    def get(self, feature_name: str) -> Optional[FeatureDefinition]:
        with self._SessionLocal() as session:
            stmt = select(FeatureDefinitionModel).where(
                FeatureDefinitionModel.feature_name == feature_name,
                FeatureDefinitionModel.is_active == True,  # noqa: E712
            )
            model = session.execute(stmt).scalar_one_or_none()
            return self._to_pydantic(model) if model else None

    def list_all(self, active_only: bool = True) -> List[FeatureDefinition]:
        with self._SessionLocal() as session:
            stmt = select(FeatureDefinitionModel)
            if active_only:
                stmt = stmt.where(FeatureDefinitionModel.is_active == True)  # noqa: E712
            models = session.execute(stmt).scalars().all()
            return [self._to_pydantic(m) for m in models]

    def update(self, feature_name: str, **kwargs) -> FeatureDefinition:
        allowed = {"description", "owner", "expected_freshness_seconds", "value_type", "is_active"}
        invalid = set(kwargs) - allowed
        if invalid:
            raise ValueError(f"Cannot update fields: {invalid}")
        with self._SessionLocal() as session:
            stmt = select(FeatureDefinitionModel).where(
                FeatureDefinitionModel.feature_name == feature_name
            )
            model = session.execute(stmt).scalar_one_or_none()
            if not model:
                raise KeyError(f"Feature '{feature_name}' not found in registry")
            for field, value in kwargs.items():
                setattr(model, field, value)
            model.updated_at = datetime.utcnow()
            session.commit()
            session.refresh(model)
            return self._to_pydantic(model)

    def delete(self, feature_name: str) -> bool:
        with self._SessionLocal() as session:
            stmt = select(FeatureDefinitionModel).where(
                FeatureDefinitionModel.feature_name == feature_name
            )
            model = session.execute(stmt).scalar_one_or_none()
            if not model:
                return False
            model.is_active = False
            model.updated_at = datetime.utcnow()
            session.commit()
            return True

    def upsert(self, definition: FeatureDefinition) -> FeatureDefinition:
        existing = self.get(definition.feature_name)
        if existing:
            return self.update(
                definition.feature_name,
                description=definition.description,
                owner=definition.owner,
                expected_freshness_seconds=definition.expected_freshness_seconds,
                value_type=definition.value_type,
                is_active=definition.is_active,
            )
        return self.create(definition)

    def ping(self) -> bool:
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    @staticmethod
    def _to_pydantic(model: FeatureDefinitionModel) -> FeatureDefinition:
        return FeatureDefinition(
            feature_name=model.feature_name,
            description=model.description or "",
            owner=model.owner,
            expected_freshness_seconds=model.expected_freshness_seconds,
            value_type=model.value_type,
            is_active=model.is_active,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
