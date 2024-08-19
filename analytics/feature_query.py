"""DuckDB feature store query layer — V2.

Reads Parquet output produced by ``spark/feature_pipeline.py`` directly via
DuckDB's native Parquet scanner (no ETL step needed).

Public API
----------
- ``get_user_features(user_id)``     — feature vector for a single user
- ``get_top_k_users(k, metric)``     — top-k users ranked by a numeric metric
- ``feature_drift_summary()``        — mean/std per feature across the dataset
- ``benchmark(n_trials)``            — query timing benchmark

All operations scan the full Parquet dataset and aim for sub-second latency
on 5 M-row datasets on a laptop.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default Parquet glob — can be overridden at construction time
DEFAULT_PARQUET_GLOB = "features/**/*.parquet"

# Metric columns available for top-k ranking
VALID_METRICS = frozenset({
    "monetary",
    "frequency",
    "recency_days",
    "rolling_7d_spend_avg",
    "anomaly_score",
    "merchant_category_count",
})


class FeatureQueryLayer:
    """DuckDB-backed query layer over the Parquet feature store.

    Parameters
    ----------
    parquet_glob:
        Glob pattern passed to ``duckdb.read_parquet()``.  Accepts the
        ``hive_partitioning=True`` flag automatically when ``features/``
        Parquet partitions are detected.
    db_path:
        Optional path to a DuckDB database file.  Defaults to in-memory.
    """

    def __init__(
        self,
        parquet_glob: str = DEFAULT_PARQUET_GLOB,
        db_path: str = ":memory:",
    ) -> None:
        import duckdb  # type: ignore[import]

        self._parquet_glob = parquet_glob
        self._con = duckdb.connect(database=db_path, read_only=False)
        self._setup_views()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _setup_views(self) -> None:
        """Create a persistent DuckDB view over the Parquet files."""
        # hive_partitioning=1 reads event_date from directory names automatically
        self._con.execute(
            f"""
            CREATE OR REPLACE VIEW feature_store AS
            SELECT * FROM read_parquet('{self._parquet_glob}', hive_partitioning=1)
            """
        )
        logger.info("DuckDB view 'feature_store' registered on '%s'", self._parquet_glob)

    def _query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        """Execute a query and return rows as list-of-dicts."""
        t0 = time.perf_counter()
        result = self._con.execute(sql, params or []).fetchdf()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug("Query completed in %.1f ms  rows=%d", elapsed_ms, len(result))
        return result.to_dict(orient="records")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_user_features(self, user_id: str) -> dict[str, Any]:
        """Return the feature vector for a single user.

        Aggregates to the most recent snapshot (max event_date) per user
        and returns all computed features as a flat dict.

        Parameters
        ----------
        user_id:
            Exact user_id string (e.g. ``"user_000042"``).

        Returns
        -------
        dict  with feature values, or empty dict if user not found.
        """
        t0 = time.perf_counter()
        rows = self._query(
            """
            SELECT
                user_id,
                AVG(rolling_7d_spend_avg)      AS rolling_7d_spend_avg,
                MAX(top_merchant_category)     AS top_merchant_category,
                MAX(merchant_category_count)   AS merchant_category_count,
                AVG(anomaly_score)             AS anomaly_score,
                MAX(recency_days)              AS recency_days,
                MAX(frequency)                 AS frequency,
                MAX(monetary)                  AS monetary
            FROM feature_store
            WHERE user_id = ?
            GROUP BY user_id
            """,
            [user_id],
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if not rows:
            logger.warning("User '%s' not found in feature store", user_id)
            return {}

        result = rows[0]
        result["query_ms"] = round(elapsed_ms, 2)
        return result

    def get_top_k_users(
        self,
        k: int = 10,
        metric: str = "monetary",
        ascending: bool = False,
    ) -> list[dict[str, Any]]:
        """Return top-k users ranked by a numeric metric.

        Parameters
        ----------
        k:
            Number of users to return.
        metric:
            One of ``VALID_METRICS``.
        ascending:
            If True, rank lowest-first (e.g. smallest recency).

        Returns
        -------
        List of dicts with user_id and the ranked metric value.
        """
        if metric not in VALID_METRICS:
            raise ValueError(
                f"metric must be one of {sorted(VALID_METRICS)}, got {metric!r}"
            )

        order_dir = "ASC" if ascending else "DESC"
        agg_col = _agg_for_metric(metric)

        rows = self._query(
            f"""
            SELECT
                user_id,
                {agg_col} AS {metric}
            FROM feature_store
            GROUP BY user_id
            ORDER BY {metric} {order_dir}
            LIMIT {int(k)}
            """
        )
        return rows

    def feature_drift_summary(self) -> dict[str, dict[str, float]]:
        """Compute mean and std for each numeric feature across the full dataset.

        Returns
        -------
        Dict mapping feature name → {"mean": ..., "std": ..., "min": ..., "max": ...}
        """
        numeric_cols = [
            "rolling_7d_spend_avg",
            "anomaly_score",
            "recency_days",
            "frequency",
            "monetary",
            "merchant_category_count",
        ]
        col_exprs = ", ".join(
            f"""
            AVG({c}) AS {c}_mean,
            STDDEV({c}) AS {c}_std,
            MIN({c}) AS {c}_min,
            MAX({c}) AS {c}_max
            """
            for c in numeric_cols
        )
        rows = self._query(f"SELECT {col_exprs} FROM feature_store")
        if not rows:
            return {}

        raw = rows[0]
        summary: dict[str, dict[str, float]] = {}
        for col in numeric_cols:
            summary[col] = {
                "mean": float(raw.get(f"{col}_mean") or 0),
                "std": float(raw.get(f"{col}_std") or 0),
                "min": float(raw.get(f"{col}_min") or 0),
                "max": float(raw.get(f"{col}_max") or 0),
            }
        return summary

    def row_count(self) -> int:
        """Return total number of rows in the feature store."""
        rows = self._query("SELECT COUNT(*) AS n FROM feature_store")
        return int(rows[0]["n"])

    # ------------------------------------------------------------------
    # Benchmark
    # ------------------------------------------------------------------

    def benchmark(self, n_trials: int = 3) -> dict[str, float]:
        """Time core queries over the full dataset.

        Runs each query ``n_trials`` times and reports min/mean/max latency.

        Returns
        -------
        Dict with keys like ``"full_scan_ms_mean"`` etc.
        """
        logger.info("Running DuckDB benchmark (%d trials each) …", n_trials)
        results: dict[str, list[float]] = {
            "full_scan_ms": [],
            "top_100_monetary_ms": [],
            "drift_summary_ms": [],
        }

        for _ in range(n_trials):
            t0 = time.perf_counter()
            self._con.execute("SELECT COUNT(*) FROM feature_store").fetchone()
            results["full_scan_ms"].append((time.perf_counter() - t0) * 1000)

            t0 = time.perf_counter()
            self.get_top_k_users(100, "monetary")
            results["top_100_monetary_ms"].append((time.perf_counter() - t0) * 1000)

            t0 = time.perf_counter()
            self.feature_drift_summary()
            results["drift_summary_ms"].append((time.perf_counter() - t0) * 1000)

        stats: dict[str, float] = {}
        for name, times in results.items():
            stats[f"{name}_min"] = round(min(times), 1)
            stats[f"{name}_mean"] = round(sum(times) / len(times), 1)
            stats[f"{name}_max"] = round(max(times), 1)

        # Print benchmark table
        print("\n=== DuckDB Query Benchmark ===")
        print(f"{'Query':<30} {'min ms':>8} {'mean ms':>8} {'max ms':>8}")
        print("-" * 58)
        for name in results:
            print(
                f"{name:<30}"
                f" {stats[name + '_min']:>8.1f}"
                f" {stats[name + '_mean']:>8.1f}"
                f" {stats[name + '_max']:>8.1f}"
            )

        total_rows = self.row_count()
        print(f"\nDataset: {total_rows:,} rows")
        sub_1s = all(stats[k] < 1000 for k in stats if "_mean" in k)
        status = "PASS (<1s)" if sub_1s else "WARN (>1s)"
        print(f"Sub-1s target: {status}")

        return stats

    def close(self) -> None:
        self._con.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _agg_for_metric(metric: str) -> str:
    """Return the appropriate aggregation expression for a metric column."""
    # recency: take MAX (most recent = smallest value, but we want the user-level stat)
    agg_map = {
        "recency_days": "MAX(recency_days)",
        "frequency": "MAX(frequency)",
        "monetary": "MAX(monetary)",
        "rolling_7d_spend_avg": "AVG(rolling_7d_spend_avg)",
        "anomaly_score": "AVG(anomaly_score)",
        "merchant_category_count": "MAX(merchant_category_count)",
    }
    return agg_map[metric]


# ---------------------------------------------------------------------------
# CLI / demo entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DuckDB feature query layer demo")
    parser.add_argument("--parquet", default="features/**/*.parquet")
    parser.add_argument("--user-id", default=None, help="Look up a specific user")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    layer = FeatureQueryLayer(parquet_glob=args.parquet)

    if args.user_id:
        print(f"\nFeatures for {args.user_id}:")
        print(layer.get_user_features(args.user_id))
    else:
        print(f"\nTop {args.top_k} users by monetary spend:")
        for row in layer.get_top_k_users(args.top_k, "monetary"):
            print(" ", row)

    print("\nFeature drift summary:")
    for feat, stats in layer.feature_drift_summary().items():
        print(f"  {feat}: {stats}")

    if args.benchmark:
        layer.benchmark()

    layer.close()
