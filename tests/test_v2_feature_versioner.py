"""Tests for store/feature_versioner.py — Iceberg-style feature versioning.

Uses DuckDB in-memory mode and does NOT require Parquet files on disk
(the versioner falls back gracefully when Parquet is unavailable).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from store.feature_versioner import (
    FeatureVersioner,
    SnapshotMeta,
    _generate_snap_id,
    _hash_schema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def versioner(tmp_path):
    """Versioner backed by a temp directory manifest store and in-memory DuckDB."""
    manifest_dir = str(tmp_path / "manifests")
    return FeatureVersioner(manifest_dir=manifest_dir, db_path=":memory:")


@pytest.fixture()
def populated_versioner(versioner):
    """Versioner with 3 committed snapshots (no real Parquet needed)."""
    versioner.commit("features/**/*.parquet", label="snap-1")
    versioner.commit("features/**/*.parquet", label="snap-2")
    versioner.commit("features/**/*.parquet", label="snap-3")
    return versioner


# ---------------------------------------------------------------------------
# commit() tests
# ---------------------------------------------------------------------------

class TestCommit:
    def test_commit_creates_snapshot(self, versioner):
        snap = versioner.commit("features/**/*.parquet", label="first")
        assert isinstance(snap, SnapshotMeta)
        assert snap.label == "first"

    def test_commit_snapshot_is_current(self, versioner):
        snap = versioner.commit("features/**/*.parquet")
        assert snap.is_current is True

    def test_commit_previous_becomes_non_current(self, versioner):
        snap1 = versioner.commit("features/**/*.parquet", label="a")
        snap2 = versioner.commit("features/**/*.parquet", label="b")
        reloaded = versioner.get_snapshot(snap1.snapshot_id)
        assert reloaded.is_current is False
        assert snap2.is_current is True

    def test_commit_sets_parent(self, versioner):
        snap1 = versioner.commit("features/**/*.parquet", label="root")
        snap2 = versioner.commit("features/**/*.parquet", label="child")
        assert snap2.parent_snapshot_id == snap1.snapshot_id

    def test_commit_first_has_no_parent(self, versioner):
        snap = versioner.commit("features/**/*.parquet")
        assert snap.parent_snapshot_id is None

    def test_commit_writes_manifest_file(self, versioner):
        snap = versioner.commit("features/**/*.parquet", label="with-manifest")
        manifest_dir = Path(versioner._manifest_dir)
        manifest_file = manifest_dir / f"snapshot_{snap.snapshot_id}.json"
        assert manifest_file.exists()

    def test_manifest_file_contains_correct_data(self, versioner):
        snap = versioner.commit("features/**/*.parquet", label="check-json")
        manifest_dir = Path(versioner._manifest_dir)
        manifest_file = manifest_dir / f"snapshot_{snap.snapshot_id}.json"
        data = json.loads(manifest_file.read_text())
        assert data["snapshot_id"] == snap.snapshot_id
        assert data["label"] == "check-json"

    def test_commit_stores_parquet_glob(self, versioner):
        glob = "my_features/**/*.parquet"
        snap = versioner.commit(glob, label="glob-test")
        reloaded = versioner.get_snapshot(snap.snapshot_id)
        assert reloaded.parquet_glob == glob


# ---------------------------------------------------------------------------
# rollback() tests
# ---------------------------------------------------------------------------

class TestRollback:
    def test_rollback_makes_target_current(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        target = snaps[-1]   # oldest (listed newest-first, so last = oldest)
        populated_versioner.rollback(target.snapshot_id)
        current = populated_versioner.current_snapshot()
        assert current.snapshot_id == target.snapshot_id

    def test_rollback_deactivates_previous_current(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        prev_current = snaps[0]
        target = snaps[-1]
        populated_versioner.rollback(target.snapshot_id)
        reloaded = populated_versioner.get_snapshot(prev_current.snapshot_id)
        assert reloaded.is_current is False

    def test_rollback_nonexistent_raises(self, versioner):
        with pytest.raises(ValueError, match="not found"):
            versioner.rollback("snap_does_not_exist")

    def test_rollback_returns_snapshot_meta(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        result = populated_versioner.rollback(snaps[-1].snapshot_id)
        assert isinstance(result, SnapshotMeta)

    def test_rollback_preserves_history(self, populated_versioner):
        count_before = len(populated_versioner.list_snapshots())
        snaps = populated_versioner.list_snapshots()
        populated_versioner.rollback(snaps[-1].snapshot_id)
        count_after = len(populated_versioner.list_snapshots())
        assert count_before == count_after   # no deletion


# ---------------------------------------------------------------------------
# diff() tests
# ---------------------------------------------------------------------------

class TestDiff:
    def test_diff_returns_required_keys(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        result = populated_versioner.diff(snaps[-1].snapshot_id, snaps[0].snapshot_id)
        for key in ("snap_a", "snap_b", "schema_changed", "row_count_a",
                    "row_count_b", "row_count_delta", "stats_diff"):
            assert key in result

    def test_diff_same_snapshot_no_change(self, populated_versioner):
        snap = populated_versioner.list_snapshots()[0]
        result = populated_versioner.diff(snap.snapshot_id, snap.snapshot_id)
        assert result["schema_changed"] is False
        assert result["row_count_delta"] == 0

    def test_diff_nonexistent_raises(self, versioner):
        snap = versioner.commit("features/**/*.parquet")
        with pytest.raises(ValueError):
            versioner.diff(snap.snapshot_id, "ghost_snap")

    def test_diff_schema_hash_a_b_populated(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        result = populated_versioner.diff(snaps[-1].snapshot_id, snaps[0].snapshot_id)
        assert result["schema_hash_a"]
        assert result["schema_hash_b"]


# ---------------------------------------------------------------------------
# list_snapshots() tests
# ---------------------------------------------------------------------------

class TestListSnapshots:
    def test_empty_versioner(self, versioner):
        assert versioner.list_snapshots() == []

    def test_correct_count(self, populated_versioner):
        assert len(populated_versioner.list_snapshots()) == 3

    def test_ordered_newest_first(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        labels = [s.label for s in snaps]
        assert labels[0] == "snap-3"   # most recent committed last

    def test_exactly_one_current(self, populated_versioner):
        snaps = populated_versioner.list_snapshots()
        current_count = sum(1 for s in snaps if s.is_current)
        assert current_count == 1


# ---------------------------------------------------------------------------
# current_snapshot() tests
# ---------------------------------------------------------------------------

class TestCurrentSnapshot:
    def test_none_when_empty(self, versioner):
        assert versioner.current_snapshot() is None

    def test_last_committed_is_current(self, versioner):
        versioner.commit("a/**/*.parquet", label="first")
        snap2 = versioner.commit("b/**/*.parquet", label="second")
        current = versioner.current_snapshot()
        assert current.snapshot_id == snap2.snapshot_id


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_hash_schema_stable(self):
        fields = [("user_id", "VARCHAR"), ("amount", "DOUBLE")]
        assert _hash_schema(fields) == _hash_schema(fields)

    def test_hash_schema_order_independent(self):
        fields_a = [("user_id", "VARCHAR"), ("amount", "DOUBLE")]
        fields_b = [("amount", "DOUBLE"), ("user_id", "VARCHAR")]
        assert _hash_schema(fields_a) == _hash_schema(fields_b)

    def test_hash_schema_differs_on_type_change(self):
        a = [("amount", "DOUBLE")]
        b = [("amount", "INTEGER")]
        assert _hash_schema(a) != _hash_schema(b)

    def test_generate_snap_id_unique(self):
        ids = {_generate_snap_id() for _ in range(50)}
        assert len(ids) >= 45   # allow tiny collision probability

    def test_generate_snap_id_format(self):
        snap_id = _generate_snap_id()
        parts = snap_id.split("_")
        assert len(parts) == 2
        assert parts[0].isdigit()
        assert len(parts[1]) == 6
