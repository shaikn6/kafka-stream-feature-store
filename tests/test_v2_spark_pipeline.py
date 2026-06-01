"""Tests for spark/feature_pipeline.py — PySpark batch feature computations.

PySpark tests use a pytest fixture that creates a local SparkSession once per
session to avoid the startup overhead.  All tests work with tiny synthetic
DataFrames so the suite stays fast.

If PySpark is not installed the entire module is skipped.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Import after pyspark check
from spark.feature_pipeline import (  # noqa: E402
    compute_anomaly_scores,
    compute_merchant_category_features,
    compute_rfm,
    compute_rolling_7d_spend,
)


# ---------------------------------------------------------------------------
# Session fixture — shared across all tests in this module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    """Local Spark session for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-feature-pipeline")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    # Do not stop — reuse across tests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TXN_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("txn_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_date", DateType(), True),
])


def _make_txn_df(spark, rows):
    """Create a transaction DataFrame from list-of-tuples."""
    data = []
    for user_id, txn_id, merchant_category, amount, event_date in rows:
        data.append((user_id, txn_id, merchant_category, float(amount), event_date))

    return spark.createDataFrame(data, schema=_TXN_SCHEMA)


# ---------------------------------------------------------------------------
# Rolling 7-day spend tests
# ---------------------------------------------------------------------------

class TestRolling7dSpend:
    def test_output_columns_present(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 100.0, date(2024, 1, 10)),
            ("u1", "t2", "grocery", 50.0, date(2024, 1, 12)),
        ])
        result = compute_rolling_7d_spend(spark, txn)
        for col in ("user_id", "txn_id", "event_date", "rolling_7d_spend_avg"):
            assert col in result.columns

    def test_single_user_single_txn(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 200.0, date(2024, 1, 5)),
        ])
        result = compute_rolling_7d_spend(spark, txn)
        row = result.collect()[0]
        assert row["rolling_7d_spend_avg"] == pytest.approx(200.0)

    def test_row_count_preserved(self, spark):
        from datetime import date
        rows = [("u1", f"t{i}", "grocery", float(i * 10), date(2024, 1, i % 28 + 1)) for i in range(1, 20)]
        txn = _make_txn_df(spark, rows)
        result = compute_rolling_7d_spend(spark, txn)
        assert result.count() == 19

    def test_rolling_avg_leq_max_amount(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u1", "t2", "grocery", 20.0, date(2024, 1, 3)),
            ("u1", "t3", "grocery", 30.0, date(2024, 1, 5)),
        ])
        result = compute_rolling_7d_spend(spark, txn)
        max_avg = result.agg(F.max("rolling_7d_spend_avg")).collect()[0][0]
        assert max_avg <= 30.0 + 0.01   # small tolerance


# ---------------------------------------------------------------------------
# Merchant category tests
# ---------------------------------------------------------------------------

class TestMerchantCategoryFeatures:
    def test_output_columns(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u1", "t2", "grocery", 10.0, date(2024, 1, 2)),
            ("u1", "t3", "dining", 10.0, date(2024, 1, 3)),
        ])
        result = compute_merchant_category_features(txn)
        for col in ("user_id", "top_merchant_category", "merchant_category_count"):
            assert col in result.columns

    def test_top_category_is_majority(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u1", "t2", "grocery", 10.0, date(2024, 1, 2)),
            ("u1", "t3", "dining", 10.0, date(2024, 1, 3)),
        ])
        result = compute_merchant_category_features(txn)
        row = result.filter(F.col("user_id") == "u1").collect()[0]
        assert row["top_merchant_category"] == "grocery"
        assert row["merchant_category_count"] == 2

    def test_one_row_per_user(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u2", "t2", "dining", 20.0, date(2024, 1, 2)),
            ("u1", "t3", "retail", 30.0, date(2024, 1, 3)),
        ])
        result = compute_merchant_category_features(txn)
        assert result.count() == 2   # one row per distinct user

    def test_tie_broken_deterministically(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u1", "t2", "dining", 10.0, date(2024, 1, 2)),
        ])
        result = compute_merchant_category_features(txn)
        # Should still produce one row
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Anomaly score tests
# ---------------------------------------------------------------------------

class TestAnomalyScores:
    def test_output_columns(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 100.0, date(2024, 1, 1)),
        ])
        result = compute_anomaly_scores(txn)
        for col in ("user_id", "txn_id", "event_date", "anomaly_score"):
            assert col in result.columns

    def test_zero_score_for_single_txn(self, spark):
        """A user with only one transaction has std=0, so z-score=0."""
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 100.0, date(2024, 1, 1)),
        ])
        result = compute_anomaly_scores(txn)
        row = result.collect()[0]
        assert row["anomaly_score"] == pytest.approx(0.0)

    def test_high_amount_gets_positive_score(self, spark):
        """A high-value txn should produce a positive z-score."""
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u1", "t2", "grocery", 10.0, date(2024, 1, 2)),
            ("u1", "t3", "grocery", 500.0, date(2024, 1, 3)),   # outlier
        ])
        result = compute_anomaly_scores(txn)
        # The outlier txn should have a positive anomaly score
        outlier = result.filter(F.col("txn_id") == "t3").collect()[0]
        assert outlier["anomaly_score"] > 0

    def test_row_count_preserved(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", f"t{i}", "grocery", float(i), date(2024, 1, i % 28 + 1))
            for i in range(1, 11)
        ])
        result = compute_anomaly_scores(txn)
        assert result.count() == 10


# ---------------------------------------------------------------------------
# RFM tests
# ---------------------------------------------------------------------------

class TestRFM:
    def test_output_columns(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 100.0, date(2024, 1, 10)),
        ])
        result = compute_rfm(txn, reference_date=date(2024, 2, 1))
        for col in ("user_id", "recency_days", "frequency", "monetary"):
            assert col in result.columns

    def test_frequency_count(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 50.0, date(2024, 1, 1)),
            ("u1", "t2", "dining", 30.0, date(2024, 1, 5)),
            ("u1", "t3", "retail", 70.0, date(2024, 1, 10)),
        ])
        result = compute_rfm(txn, reference_date=date(2024, 2, 1))
        row = result.filter(F.col("user_id") == "u1").collect()[0]
        assert row["frequency"] == 3

    def test_monetary_sum(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 50.0, date(2024, 1, 1)),
            ("u1", "t2", "dining", 30.0, date(2024, 1, 5)),
        ])
        result = compute_rfm(txn, reference_date=date(2024, 2, 1))
        row = result.filter(F.col("user_id") == "u1").collect()[0]
        assert row["monetary"] == pytest.approx(80.0)

    def test_recency_is_days_since_last_txn(self, spark):
        from datetime import date
        ref = date(2024, 2, 10)
        last_txn = date(2024, 2, 5)   # 5 days before reference
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 50.0, last_txn),
        ])
        result = compute_rfm(txn, reference_date=ref)
        row = result.collect()[0]
        assert row["recency_days"] == 5

    def test_one_row_per_user(self, spark):
        from datetime import date
        txn = _make_txn_df(spark, [
            ("u1", "t1", "grocery", 10.0, date(2024, 1, 1)),
            ("u2", "t2", "dining", 20.0, date(2024, 1, 2)),
            ("u1", "t3", "retail", 30.0, date(2024, 1, 3)),
        ])
        result = compute_rfm(txn, reference_date=date(2024, 2, 1))
        assert result.count() == 2
