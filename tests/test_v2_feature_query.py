"""Tests for analytics/feature_query.py — DuckDB feature query layer.

Uses DuckDB's in-memory mode with synthetic data inserted directly via SQL,
so no Parquet files or Spark are required.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from analytics.feature_query import (
    FeatureQueryLayer,
    VALID_METRICS,
    _agg_for_metric,
)


# ---------------------------------------------------------------------------
# Fixtures — inject synthetic data without Parquet files
# ---------------------------------------------------------------------------

_SEED_SQL = """
CREATE OR REPLACE TABLE synthetic_features AS
SELECT
    'user_' || LPAD(CAST(i % 1000 AS VARCHAR), 6, '0')   AS user_id,
    'txn_' || CAST(i AS VARCHAR)                          AS txn_id,
    CAST(100.0 + (i % 500) AS DOUBLE)                     AS rolling_7d_spend_avg,
    CASE i % 4
        WHEN 0 THEN 'grocery'
        WHEN 1 THEN 'dining'
        WHEN 2 THEN 'travel'
        ELSE        'retail'
    END                                                    AS top_merchant_category,
    CAST(5 + (i % 100) AS INTEGER)                        AS merchant_category_count,
    CAST((i % 6) * 0.3 - 0.7 AS DOUBLE)                  AS anomaly_score,
    CAST(1 + (i % 30) AS INTEGER)                         AS recency_days,
    CAST(10 + (i % 200) AS INTEGER)                       AS frequency,
    CAST(500.0 + (i % 10000) AS DOUBLE)                   AS monetary,
    DATE '2024-01-01' + CAST(i % 30 AS INTEGER)           AS event_date
FROM range(50000) t(i)
"""


@pytest.fixture()
def query_layer():
    """FeatureQueryLayer pointing at an in-memory DuckDB with synthetic data."""
    layer = FeatureQueryLayer.__new__(FeatureQueryLayer)

    import duckdb
    layer._parquet_glob = "synthetic_features"
    layer._con = duckdb.connect(":memory:")
    layer._con.execute(_SEED_SQL)

    # Override the view to use the in-memory table instead of Parquet
    layer._con.execute(
        "CREATE OR REPLACE VIEW feature_store AS SELECT * FROM synthetic_features"
    )
    return layer


# ---------------------------------------------------------------------------
# get_user_features() tests
# ---------------------------------------------------------------------------

class TestGetUserFeatures:
    def test_known_user_returns_dict(self, query_layer):
        result = query_layer.get_user_features("user_000001")
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_returned_dict_has_expected_keys(self, query_layer):
        result = query_layer.get_user_features("user_000001")
        for key in ("user_id", "rolling_7d_spend_avg", "monetary", "frequency",
                    "recency_days", "anomaly_score"):
            assert key in result, f"Missing key: {key}"

    def test_unknown_user_returns_empty_dict(self, query_layer):
        result = query_layer.get_user_features("user_999999")
        assert result == {}

    def test_user_id_in_result(self, query_layer):
        result = query_layer.get_user_features("user_000042")
        assert result["user_id"] == "user_000042"

    def test_query_ms_populated(self, query_layer):
        result = query_layer.get_user_features("user_000001")
        assert "query_ms" in result
        assert result["query_ms"] >= 0

    def test_numeric_values_are_numbers(self, query_layer):
        result = query_layer.get_user_features("user_000001")
        for key in ("rolling_7d_spend_avg", "monetary", "frequency"):
            assert isinstance(result[key], (int, float)), f"{key} is not numeric"


# ---------------------------------------------------------------------------
# get_top_k_users() tests
# ---------------------------------------------------------------------------

class TestGetTopKUsers:
    def test_returns_k_rows(self, query_layer):
        result = query_layer.get_top_k_users(k=5, metric="monetary")
        assert len(result) == 5

    def test_monetary_descending_by_default(self, query_layer):
        result = query_layer.get_top_k_users(k=10, metric="monetary")
        values = [r["monetary"] for r in result]
        assert values == sorted(values, reverse=True)

    def test_ascending_option(self, query_layer):
        result = query_layer.get_top_k_users(k=10, metric="frequency", ascending=True)
        values = [r["frequency"] for r in result]
        assert values == sorted(values)

    def test_all_valid_metrics_work(self, query_layer):
        for metric in VALID_METRICS:
            rows = query_layer.get_top_k_users(k=3, metric=metric)
            assert len(rows) <= 3, f"metric {metric} returned too many rows"

    def test_invalid_metric_raises(self, query_layer):
        with pytest.raises(ValueError, match="metric must be one of"):
            query_layer.get_top_k_users(k=5, metric="nonexistent_metric")

    def test_top_1_returns_single_row(self, query_layer):
        result = query_layer.get_top_k_users(k=1, metric="monetary")
        assert len(result) == 1

    def test_result_contains_user_id(self, query_layer):
        result = query_layer.get_top_k_users(k=5, metric="monetary")
        for row in result:
            assert "user_id" in row


# ---------------------------------------------------------------------------
# feature_drift_summary() tests
# ---------------------------------------------------------------------------

class TestFeatureDriftSummary:
    def test_returns_dict(self, query_layer):
        result = query_layer.feature_drift_summary()
        assert isinstance(result, dict)

    def test_expected_features_present(self, query_layer):
        result = query_layer.feature_drift_summary()
        for feat in ("rolling_7d_spend_avg", "anomaly_score", "monetary", "frequency"):
            assert feat in result

    def test_each_feature_has_stats(self, query_layer):
        result = query_layer.feature_drift_summary()
        for feat, stats in result.items():
            for stat in ("mean", "std", "min", "max"):
                assert stat in stats, f"Missing {stat} in {feat}"

    def test_mean_between_min_and_max(self, query_layer):
        result = query_layer.feature_drift_summary()
        for feat, stats in result.items():
            assert stats["min"] <= stats["mean"] <= stats["max"], (
                f"mean out of range for {feat}"
            )


# ---------------------------------------------------------------------------
# row_count() tests
# ---------------------------------------------------------------------------

class TestRowCount:
    def test_row_count_matches_seed(self, query_layer):
        assert query_layer.row_count() == 50_000


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestHelpers:
    @pytest.mark.parametrize("metric", list(VALID_METRICS))
    def test_agg_for_metric_returns_string(self, metric):
        expr = _agg_for_metric(metric)
        assert isinstance(expr, str)
        assert len(expr) > 0

    def test_agg_for_metric_contains_column(self):
        expr = _agg_for_metric("monetary")
        assert "monetary" in expr.lower()


# ---------------------------------------------------------------------------
# Benchmark (light version for CI — not timing-sensitive)
# ---------------------------------------------------------------------------

class TestBenchmark:
    def test_benchmark_returns_stats_dict(self, query_layer):
        stats = query_layer.benchmark(n_trials=1)
        assert isinstance(stats, dict)
        for suffix in ("_min", "_mean", "_max"):
            assert any(k.endswith(suffix) for k in stats)

    def test_benchmark_timings_positive(self, query_layer):
        stats = query_layer.benchmark(n_trials=1)
        for val in stats.values():
            assert val >= 0
