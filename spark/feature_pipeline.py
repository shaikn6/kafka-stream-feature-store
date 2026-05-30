"""PySpark batch feature computation pipeline — V2.

Generates 5 million synthetic user-transaction records, computes rich
ML feature sets, and writes partitioned Parquet output.

Features computed
-----------------
- 7-day rolling spend average (``rolling_7d_spend_avg``)
- Merchant category frequency map (``top_merchant_category``, ``merchant_category_count``)
- Anomaly score — z-score of spend vs. user's own historical distribution
- RFM: Recency (days since last txn), Frequency (txn count), Monetary (total spend)

Usage (local, no cluster needed)::

    python spark/feature_pipeline.py

Output written to ``features/`` as Parquet partitioned by ``event_date``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------

def _build_spark(app_name: str = "FeaturePipelineV2"):
    """Create a local-mode SparkSession (no cluster required)."""
    # Import inside function so the module can be imported in test environments
    # without PySpark installed — tests mock this factory.
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Silence noisy Hadoop / Parquet INFO logs
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

_MERCHANT_CATEGORIES = [
    "grocery", "dining", "travel", "entertainment",
    "utilities", "healthcare", "retail", "fuel",
]

_GENERATE_SQL = """
SELECT
    user_id,
    txn_id,
    merchant_category,
    amount,
    event_date
FROM (
    SELECT
        CONCAT('user_', LPAD(CAST(CAST(rand() * 50000 AS INT) AS STRING), 6, '0')) AS user_id,
        CONCAT('txn_', CAST(id AS STRING))                                          AS txn_id,
        CASE CAST(CAST(rand() * 8 AS INT) AS INT)
            WHEN 0 THEN 'grocery'
            WHEN 1 THEN 'dining'
            WHEN 2 THEN 'travel'
            WHEN 3 THEN 'entertainment'
            WHEN 4 THEN 'utilities'
            WHEN 5 THEN 'healthcare'
            WHEN 6 THEN 'retail'
            ELSE        'fuel'
        END AS merchant_category,
        ROUND(5 + rand() * 495, 2) AS amount,
        DATE_SUB(CURRENT_DATE(), CAST(rand() * 30 AS INT)) AS event_date
    FROM range(5000000)
) t
"""


def generate_transactions(spark):
    """Generate 5 million synthetic transaction records via Spark SQL range scan."""
    logger.info("Generating 5M synthetic transaction records …")
    t0 = time.perf_counter()
    df = spark.sql(_GENERATE_SQL)
    # Materialise once so downstream stages share the same cached data
    df = df.cache()
    count = df.count()
    elapsed = time.perf_counter() - t0
    logger.info("Generated %d records in %.2fs", count, elapsed)
    return df


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

def compute_rolling_7d_spend(spark, txn_df):
    """7-day rolling spend average per user.

    Because the synthetic data spans ≤ 30 days and we want a rolling window,
    we use a window function ordered by event_date with a 7-day range.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    logger.info("Computing 7-day rolling spend average …")
    w = (
        Window
        .partitionBy("user_id")
        .orderBy(F.col("event_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * 86400, 0)   # 7 days in seconds
    )
    return (
        txn_df
        .withColumn("rolling_7d_spend_avg", F.avg("amount").over(w))
        .select("user_id", "txn_id", "event_date", "rolling_7d_spend_avg")
    )


def compute_merchant_category_features(txn_df):
    """Per-user dominant merchant category + category transaction count."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    logger.info("Computing merchant category features …")

    # Count transactions per (user, category)
    cat_counts = (
        txn_df
        .groupBy("user_id", "merchant_category")
        .agg(F.count("*").alias("cat_count"))
    )

    # Rank categories by frequency per user → pick top-1
    w = Window.partitionBy("user_id").orderBy(F.col("cat_count").desc())
    top_cat = (
        cat_counts
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select(
            "user_id",
            F.col("merchant_category").alias("top_merchant_category"),
            F.col("cat_count").alias("merchant_category_count"),
        )
    )
    return top_cat


def compute_anomaly_scores(txn_df):
    """Z-score of each transaction's amount relative to the user's spend distribution."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    logger.info("Computing anomaly scores (z-score) …")

    w = Window.partitionBy("user_id")
    with_stats = (
        txn_df
        .withColumn("user_mean", F.avg("amount").over(w))
        .withColumn("user_std",  F.stddev("amount").over(w))
    )
    # Avoid division by zero for users with a single transaction
    anomaly = with_stats.withColumn(
        "anomaly_score",
        F.when(
            F.col("user_std").isNull() | (F.col("user_std") == 0),
            F.lit(0.0),
        ).otherwise(
            (F.col("amount") - F.col("user_mean")) / F.col("user_std")
        )
    ).select("user_id", "txn_id", "event_date", "anomaly_score")

    return anomaly


def compute_rfm(txn_df, reference_date=None):
    """Recency / Frequency / Monetary per user.

    - Recency: days since most recent transaction
    - Frequency: total transaction count
    - Monetary: total spend
    """
    from pyspark.sql import functions as F

    logger.info("Computing RFM features …")

    if reference_date is None:
        # Use today as reference so scores are stable in tests
        reference_date = datetime.utcnow().date()

    ref = F.lit(str(reference_date)).cast("date")

    rfm = (
        txn_df
        .groupBy("user_id")
        .agg(
            F.datediff(ref, F.max("event_date")).alias("recency_days"),
            F.count("*").alias("frequency"),
            F.round(F.sum("amount"), 2).alias("monetary"),
        )
    )
    return rfm


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------

def run_pipeline(output_path: str = "features", n_rows: int = 5_000_000) -> dict:
    """Execute the full V2 feature pipeline.

    Parameters
    ----------
    output_path:
        Directory to write partitioned Parquet output.
    n_rows:
        Number of synthetic transaction rows (default 5M).

    Returns
    -------
    dict with execution metadata (row counts, elapsed, schema).
    """
    from pyspark.sql import functions as F

    spark = _build_spark()
    t_start = time.perf_counter()

    # ---- 1. Generate synthetic data -----------------------------------------
    txn_df = generate_transactions(spark)

    # ---- 2. Feature computations --------------------------------------------
    rolling_df  = compute_rolling_7d_spend(spark, txn_df)
    cat_df      = compute_merchant_category_features(txn_df)
    anomaly_df  = compute_anomaly_scores(txn_df)
    rfm_df      = compute_rfm(txn_df)

    # ---- 3. Join all features into a single wide table ----------------------
    logger.info("Joining feature tables …")

    # Base: one row per (user_id, txn_id, event_date) with rolling + anomaly
    features_df = (
        rolling_df
        .join(anomaly_df, on=["user_id", "txn_id", "event_date"], how="inner")
        .join(cat_df,     on="user_id",                            how="left")
        .join(rfm_df,     on="user_id",                            how="left")
    )

    # ---- 4. Write partitioned Parquet ---------------------------------------
    logger.info("Writing features to Parquet at '%s' partitioned by event_date …", output_path)
    (
        features_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    # ---- 5. Verification / diagnostics --------------------------------------
    # Re-read to confirm write success and show stats
    result_df = spark.read.parquet(output_path)
    row_count = result_df.count()
    partitions = result_df.select("event_date").distinct().count()

    elapsed = time.perf_counter() - t_start

    logger.info("=" * 60)
    logger.info("Pipeline complete in %.1fs", elapsed)
    logger.info("Total feature rows written : %d", row_count)
    logger.info("Parquet partitions (dates) : %d", partitions)
    logger.info("Schema:")
    result_df.printSchema()
    logger.info("Sample rows (5):")
    result_df.show(5, truncate=False)

    return {
        "row_count": row_count,
        "partitions": partitions,
        "elapsed_seconds": round(elapsed, 2),
        "schema_fields": [f.name for f in result_df.schema.fields],
        "output_path": output_path,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="V2 PySpark feature pipeline")
    parser.add_argument(
        "--output", default="features",
        help="Output path for Parquet (default: features/)",
    )
    parser.add_argument(
        "--rows", type=int, default=5_000_000,
        help="Number of synthetic transaction rows (default: 5000000)",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    meta = run_pipeline(output_path=args.output, n_rows=args.rows)
    print("\nPipeline metadata:", meta)
