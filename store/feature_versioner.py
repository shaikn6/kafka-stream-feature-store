"""Apache Iceberg-style feature versioning — V2.

Simulates Iceberg snapshot semantics using DuckDB as the storage engine and
JSON manifest files as the metadata layer.

Snapshot lifecycle
------------------
1. ``commit(parquet_glob, label)`` — create a new snapshot from current Parquet
2. ``rollback(snapshot_id)``       — make a previous snapshot the "current" one
3. ``diff(snap_a, snap_b)``        — compare schema + aggregate stats between snaps
4. ``list_snapshots()``            — ordered snapshot history

Each snapshot record persists in a DuckDB table (``snapshots``) and a companion
JSON manifest at ``{manifest_dir}/snapshot_{id}.json``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class SnapshotMeta:
    """Metadata stored per snapshot."""

    snapshot_id: str
    label: str
    created_at: str                     # ISO-8601 UTC
    parquet_glob: str
    schema_hash: str                    # SHA-256 of sorted field names + types
    row_count: int
    stats: dict[str, Any]              # per-column mean/min/max
    parent_snapshot_id: Optional[str] = None
    is_current: bool = False


# ---------------------------------------------------------------------------
# Versioner
# ---------------------------------------------------------------------------

class FeatureVersioner:
    """Iceberg-style snapshot manager for the DuckDB-backed feature store.

    Parameters
    ----------
    manifest_dir:
        Directory where JSON manifests are written.
    db_path:
        DuckDB database file.  Defaults to in-memory (useful for tests).
    """

    def __init__(
        self,
        manifest_dir: str = "store/manifests",
        db_path: str = ":memory:",
    ) -> None:
        import duckdb  # type: ignore[import]

        self._manifest_dir = Path(manifest_dir)
        self._manifest_dir.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(database=db_path, read_only=False)
        self._bootstrap_schema()

    # ------------------------------------------------------------------
    # Schema bootstrap
    # ------------------------------------------------------------------

    def _bootstrap_schema(self) -> None:
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id      VARCHAR PRIMARY KEY,
                label            VARCHAR,
                created_at       TIMESTAMP,
                parquet_glob     VARCHAR,
                schema_hash      VARCHAR,
                row_count        BIGINT,
                stats            JSON,
                parent_snapshot_id VARCHAR,
                is_current       BOOLEAN DEFAULT FALSE
            )
            """
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def commit(
        self,
        parquet_glob: str,
        label: str = "",
    ) -> SnapshotMeta:
        """Create a new snapshot from the current Parquet files.

        Steps:
        1. Read Parquet via DuckDB to capture schema + row stats.
        2. Generate a schema hash.
        3. Insert snapshot record into DuckDB.
        4. Write JSON manifest to ``manifest_dir``.
        5. Mark this snapshot as current (unmarks previous current).

        Returns
        -------
        SnapshotMeta for the new snapshot.
        """
        logger.info("Committing new snapshot from '%s' …", parquet_glob)

        schema_fields, row_count, stats = self._scan_parquet(parquet_glob)
        schema_hash = _hash_schema(schema_fields)
        snap_id = _generate_snap_id()
        created_at = datetime.now(tz=timezone.utc).isoformat()

        # Determine parent
        current = self._current_snapshot_id()

        # Deactivate previous current
        self._con.execute("UPDATE snapshots SET is_current = FALSE WHERE is_current = TRUE")

        # Insert new snapshot
        self._con.execute(
            """
            INSERT INTO snapshots
                (snapshot_id, label, created_at, parquet_glob, schema_hash,
                 row_count, stats, parent_snapshot_id, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
            """,
            [
                snap_id, label, created_at, parquet_glob, schema_hash,
                row_count, json.dumps(stats), current,
            ],
        )

        meta = SnapshotMeta(
            snapshot_id=snap_id,
            label=label,
            created_at=created_at,
            parquet_glob=parquet_glob,
            schema_hash=schema_hash,
            row_count=row_count,
            stats=stats,
            parent_snapshot_id=current,
            is_current=True,
        )

        # Write JSON manifest
        manifest_path = self._manifest_dir / f"snapshot_{snap_id}.json"
        manifest_path.write_text(json.dumps(asdict(meta), indent=2))
        logger.info("Snapshot %s committed  rows=%d  manifest=%s", snap_id, row_count, manifest_path)
        return meta

    def rollback(self, snapshot_id: str) -> SnapshotMeta:
        """Make a previous snapshot the current one.

        Does NOT delete any data — Iceberg semantics preserve all history.

        Parameters
        ----------
        snapshot_id:
            ID of the snapshot to restore.

        Returns
        -------
        SnapshotMeta of the restored snapshot.

        Raises
        ------
        ValueError if snapshot_id does not exist.
        """
        row = self._con.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?", [snapshot_id]
        ).fetchone()

        if row is None:
            raise ValueError(f"Snapshot '{snapshot_id}' not found")

        self._con.execute("UPDATE snapshots SET is_current = FALSE WHERE is_current = TRUE")
        self._con.execute(
            "UPDATE snapshots SET is_current = TRUE WHERE snapshot_id = ?",
            [snapshot_id],
        )
        logger.info("Rolled back to snapshot %s", snapshot_id)
        return self.get_snapshot(snapshot_id)

    def diff(self, snap_a: str, snap_b: str) -> dict[str, Any]:
        """Compare two snapshots — schema changes + statistical drift.

        Parameters
        ----------
        snap_a, snap_b:
            Snapshot IDs to compare (order matters for delta sign).

        Returns
        -------
        Dict with:
        - ``schema_changed``: bool
        - ``schema_hash_a``, ``schema_hash_b``
        - ``row_count_a``, ``row_count_b``, ``row_count_delta``
        - ``stats_diff``: per-feature {"mean_delta": ..., "std_delta": ...}
        """
        meta_a = self.get_snapshot(snap_a)
        meta_b = self.get_snapshot(snap_b)

        stats_diff: dict[str, dict[str, float]] = {}
        all_features = set(meta_a.stats.keys()) | set(meta_b.stats.keys())
        for feat in all_features:
            a_stat = meta_a.stats.get(feat, {})
            b_stat = meta_b.stats.get(feat, {})
            stats_diff[feat] = {
                "mean_a": a_stat.get("mean", 0),
                "mean_b": b_stat.get("mean", 0),
                "mean_delta": round(b_stat.get("mean", 0) - a_stat.get("mean", 0), 4),
                "std_a": a_stat.get("std", 0),
                "std_b": b_stat.get("std", 0),
                "std_delta": round(b_stat.get("std", 0) - a_stat.get("std", 0), 4),
            }

        return {
            "snap_a": snap_a,
            "snap_b": snap_b,
            "schema_changed": meta_a.schema_hash != meta_b.schema_hash,
            "schema_hash_a": meta_a.schema_hash,
            "schema_hash_b": meta_b.schema_hash,
            "row_count_a": meta_a.row_count,
            "row_count_b": meta_b.row_count,
            "row_count_delta": meta_b.row_count - meta_a.row_count,
            "stats_diff": stats_diff,
        }

    def list_snapshots(self) -> list[SnapshotMeta]:
        """Return all snapshots ordered newest-first."""
        rows = self._con.execute(
            "SELECT * FROM snapshots ORDER BY created_at DESC"
        ).fetchdf()
        return [
            SnapshotMeta(
                snapshot_id=r["snapshot_id"],
                label=r["label"] or "",
                created_at=str(r["created_at"]),
                parquet_glob=r["parquet_glob"],
                schema_hash=r["schema_hash"],
                row_count=int(r["row_count"]),
                stats=json.loads(r["stats"]) if isinstance(r["stats"], str) else r["stats"],
                parent_snapshot_id=r.get("parent_snapshot_id"),
                is_current=bool(r["is_current"]),
            )
            for _, r in rows.iterrows()
        ]

    def get_snapshot(self, snapshot_id: str) -> SnapshotMeta:
        """Retrieve a single snapshot by ID."""
        row = self._con.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?", [snapshot_id]
        ).fetchdf()
        if row.empty:
            raise ValueError(f"Snapshot '{snapshot_id}' not found")
        r = row.iloc[0]
        return SnapshotMeta(
            snapshot_id=r["snapshot_id"],
            label=r["label"] or "",
            created_at=str(r["created_at"]),
            parquet_glob=r["parquet_glob"],
            schema_hash=r["schema_hash"],
            row_count=int(r["row_count"]),
            stats=json.loads(r["stats"]) if isinstance(r["stats"], str) else r["stats"],
            parent_snapshot_id=r.get("parent_snapshot_id"),
            is_current=bool(r["is_current"]),
        )

    def current_snapshot(self) -> Optional[SnapshotMeta]:
        """Return the currently active snapshot, or None if none exists."""
        snap_id = self._current_snapshot_id()
        if snap_id is None:
            return None
        return self.get_snapshot(snap_id)

    def close(self) -> None:
        self._con.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _current_snapshot_id(self) -> Optional[str]:
        row = self._con.execute(
            "SELECT snapshot_id FROM snapshots WHERE is_current = TRUE LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    def _scan_parquet(
        self,
        parquet_glob: str,
    ) -> tuple[list[tuple[str, str]], int, dict[str, Any]]:
        """Read Parquet and return (schema_fields, row_count, stats)."""
        import duckdb  # type: ignore[import]

        try:
            tmp_con = duckdb.connect(":memory:")
            tmp_con.execute(
                f"""
                CREATE OR REPLACE VIEW _scan AS
                SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=1)
                """
            )
            count_row = tmp_con.execute("SELECT COUNT(*) FROM _scan").fetchone()
            row_count = int(count_row[0]) if count_row else 0

            # Schema
            col_info = tmp_con.execute("DESCRIBE _scan").fetchdf()
            schema_fields = [
                (r["column_name"], r["column_type"])
                for _, r in col_info.iterrows()
            ]

            # Stats for numeric columns
            numeric_cols = [
                c for c, t in schema_fields
                if any(k in t.upper() for k in ("DOUBLE", "FLOAT", "INT", "DECIMAL", "BIGINT"))
                and c not in ("event_date",)
            ]

            stats: dict[str, Any] = {}
            if numeric_cols and row_count > 0:
                exprs = ", ".join(
                    f"AVG({c}) AS {c}_mean, STDDEV({c}) AS {c}_std, "
                    f"MIN({c}) AS {c}_min, MAX({c}) AS {c}_max"
                    for c in numeric_cols
                )
                row = tmp_con.execute(f"SELECT {exprs} FROM _scan").fetchdf().iloc[0]
                for col in numeric_cols:
                    stats[col] = {
                        "mean": float(row.get(f"{col}_mean") or 0),
                        "std": float(row.get(f"{col}_std") or 0),
                        "min": float(row.get(f"{col}_min") or 0),
                        "max": float(row.get(f"{col}_max") or 0),
                    }

            tmp_con.close()
            return schema_fields, row_count, stats

        except Exception as exc:
            # Parquet files may not exist yet in test scenarios
            logger.warning("Could not scan Parquet '%s': %s — using empty stats", parquet_glob, exc)
            return [], 0, {}


# ---------------------------------------------------------------------------
# Pure helpers (no external deps)
# ---------------------------------------------------------------------------

def _hash_schema(schema_fields: list[tuple[str, str]]) -> str:
    """Return a stable SHA-256 hash of the schema field names and types."""
    raw = json.dumps(sorted(schema_fields), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def _generate_snap_id() -> str:
    """Generate a time-ordered snapshot ID (epoch_ms + short random suffix)."""
    ts = int(time.time() * 1000)
    import random
    suffix = "".join(random.choices("abcdef0123456789", k=6))
    return f"{ts:013d}_{suffix}"


# ---------------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Iceberg-style feature versioner demo")
    parser.add_argument("--parquet", default="features/**/*.parquet")
    parser.add_argument("--manifest-dir", default="store/manifests")
    args = parser.parse_args()

    versioner = FeatureVersioner(manifest_dir=args.manifest_dir)

    # Commit initial snapshot
    snap1 = versioner.commit(args.parquet, label="initial-load")
    print(f"\nSnapshot 1: {snap1.snapshot_id}  rows={snap1.row_count}")

    # Commit a second (simulates incremental update)
    snap2 = versioner.commit(args.parquet, label="incremental-update")
    print(f"Snapshot 2: {snap2.snapshot_id}  rows={snap2.row_count}")

    # Diff
    delta = versioner.diff(snap1.snapshot_id, snap2.snapshot_id)
    print(f"\nDiff snap1→snap2:  schema_changed={delta['schema_changed']}  "
          f"row_delta={delta['row_count_delta']}")

    # History
    print("\nSnapshot history:")
    for s in versioner.list_snapshots():
        current_marker = " ← current" if s.is_current else ""
        print(f"  {s.snapshot_id}  {s.label}  rows={s.row_count}{current_marker}")

    # Rollback
    versioner.rollback(snap1.snapshot_id)
    print(f"\nRolled back to {snap1.snapshot_id}")
    print(f"Current: {versioner.current_snapshot().snapshot_id}")
    versioner.close()
