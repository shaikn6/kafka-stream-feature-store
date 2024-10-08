"""Tests for feature_store/registry.py — FeatureRegistry CRUD using SQLite."""
from __future__ import annotations

import pytest

from feature_store.registry import FeatureRegistry
from feature_store.schemas.feature_event import FeatureDefinition


@pytest.fixture
def registry(tmp_path):
    db_path = tmp_path / "test_registry.db"
    reg = FeatureRegistry(db_url=f"sqlite:///{db_path}")
    reg.create_tables()
    yield reg
    reg.drop_tables()


def _make_def(**kwargs) -> FeatureDefinition:
    defaults = dict(
        feature_name="test_feature",
        description="A test feature",
        owner="team_ml",
        expected_freshness_seconds=60,
        value_type="float",
        is_active=True,
    )
    defaults.update(kwargs)
    return FeatureDefinition(**defaults)


class TestFeatureRegistryCreate:
    def test_create_returns_definition(self, registry):
        result = registry.create(_make_def())
        assert result.feature_name == "test_feature"

    def test_create_sets_owner(self, registry):
        result = registry.create(_make_def(owner="data_team"))
        assert result.owner == "data_team"

    def test_create_sets_freshness(self, registry):
        result = registry.create(_make_def(expected_freshness_seconds=120))
        assert result.expected_freshness_seconds == 120

    def test_create_sets_value_type(self, registry):
        result = registry.create(_make_def(value_type="int"))
        assert result.value_type == "int"

    def test_create_duplicate_raises(self, registry):
        registry.create(_make_def())
        with pytest.raises(ValueError, match="already exists"):
            registry.create(_make_def())

    def test_create_multiple_distinct(self, registry):
        registry.create(_make_def(feature_name="f1"))
        registry.create(_make_def(feature_name="f2"))
        assert registry.get("f1") is not None
        assert registry.get("f2") is not None


class TestFeatureRegistryGet:
    def test_get_existing(self, registry):
        registry.create(_make_def())
        result = registry.get("test_feature")
        assert result is not None
        assert result.feature_name == "test_feature"

    def test_get_missing_returns_none(self, registry):
        assert registry.get("nonexistent") is None

    def test_get_returns_correct_description(self, registry):
        registry.create(_make_def(description="my desc"))
        result = registry.get("test_feature")
        assert result.description == "my desc"


class TestFeatureRegistryListAll:
    def test_list_empty(self, registry):
        assert registry.list_all() == []

    def test_list_returns_created(self, registry):
        registry.create(_make_def(feature_name="f1"))
        registry.create(_make_def(feature_name="f2"))
        results = registry.list_all()
        assert len(results) == 2

    def test_list_active_only_default(self, registry):
        registry.create(_make_def(feature_name="active"))
        registry.create(_make_def(feature_name="inactive"))
        registry.delete("inactive")
        active = registry.list_all(active_only=True)
        assert len(active) == 1
        assert active[0].feature_name == "active"

    def test_list_all_includes_inactive(self, registry):
        registry.create(_make_def(feature_name="active"))
        registry.create(_make_def(feature_name="inactive"))
        registry.delete("inactive")
        all_results = registry.list_all(active_only=False)
        assert len(all_results) == 2

    def test_list_returns_feature_objects(self, registry):
        registry.create(_make_def())
        results = registry.list_all()
        assert all(isinstance(r, FeatureDefinition) for r in results)


class TestFeatureRegistryUpdate:
    def test_update_description(self, registry):
        registry.create(_make_def())
        updated = registry.update("test_feature", description="new description")
        assert updated.description == "new description"

    def test_update_freshness_seconds(self, registry):
        registry.create(_make_def())
        updated = registry.update("test_feature", expected_freshness_seconds=120)
        assert updated.expected_freshness_seconds == 120

    def test_update_owner(self, registry):
        registry.create(_make_def())
        updated = registry.update("test_feature", owner="new_team")
        assert updated.owner == "new_team"

    def test_update_is_active(self, registry):
        registry.create(_make_def())
        updated = registry.update("test_feature", is_active=False)
        assert updated.is_active is False

    def test_update_nonexistent_raises(self, registry):
        with pytest.raises(KeyError):
            registry.update("nonexistent", description="x")

    def test_update_invalid_field_raises(self, registry):
        registry.create(_make_def())
        with pytest.raises(ValueError, match="Cannot update"):
            registry.update("test_feature", unknown_field="val")

    def test_update_persists(self, registry):
        registry.create(_make_def())
        registry.update("test_feature", description="persisted")
        fetched = registry.get("test_feature")
        assert fetched.description == "persisted"


class TestFeatureRegistryDelete:
    def test_delete_existing_returns_true(self, registry):
        registry.create(_make_def())
        assert registry.delete("test_feature") is True

    def test_delete_marks_inactive(self, registry):
        registry.create(_make_def())
        registry.delete("test_feature")
        assert registry.get("test_feature") is None

    def test_delete_nonexistent_returns_false(self, registry):
        assert registry.delete("nonexistent") is False

    def test_delete_does_not_remove_others(self, registry):
        registry.create(_make_def(feature_name="keep"))
        registry.create(_make_def(feature_name="remove"))
        registry.delete("remove")
        assert registry.get("keep") is not None


class TestFeatureRegistryUpsert:
    def test_upsert_creates_new(self, registry):
        result = registry.upsert(_make_def())
        assert result.feature_name == "test_feature"

    def test_upsert_updates_existing(self, registry):
        registry.create(_make_def(description="old"))
        result = registry.upsert(_make_def(description="new"))
        assert result.description == "new"

    def test_upsert_twice_no_error(self, registry):
        registry.upsert(_make_def(description="v1"))
        registry.upsert(_make_def(description="v2"))
        assert registry.get("test_feature").description == "v2"


class TestFeatureRegistryPing:
    def test_ping_returns_true(self, registry):
        assert registry.ping() is True
