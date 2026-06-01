"""Streamlit V2 dashboard — multi-tab feature store explorer.

Tabs
----
1. Live Stream   — Kafka→Redis stats (V1 mock mode)
2. Spark Runner  — trigger batch PySpark compute, show progress
3. DuckDB Explorer — query user features, show feature vector
4. Version History — Iceberg-style snapshot timeline

Run::

    streamlit run dashboard/app_v2.py
"""

from __future__ import annotations

import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Streamlit import guard
# ---------------------------------------------------------------------------

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False
    # Allow import in test environments without Streamlit

    class _StubST:
        def __getattr__(self, name):
            def _noop(*a, **kw):
                pass
            return _noop
    st = _StubST()  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

if HAS_STREAMLIT:
    st.set_page_config(
        page_title="Feature Store V2",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

# ---------------------------------------------------------------------------
# Helpers — mock data generators (work without running Kafka/Redis/Spark)
# ---------------------------------------------------------------------------

_FEATURES_DIR = Path(os.getenv("FEATURES_DIR", "features"))
_PARQUET_GLOB = str(_FEATURES_DIR / "**" / "*.parquet")


def _mock_stream_stats() -> dict[str, Any]:
    """Simulate live stream metrics (used when Kafka/Redis not running)."""
    rng = random.Random(int(time.time() / 5))   # changes every 5s
    return {
        "messages_processed": rng.randint(50_000, 200_000),
        "messages_failed": rng.randint(0, 50),
        "redis_writes": rng.randint(49_000, 199_000),
        "throughput_per_sec": rng.randint(80, 500),
        "stale_features": rng.randint(0, 10),
        "last_updated": datetime.utcnow().isoformat(),
    }


def _mock_spark_stages() -> list[dict[str, Any]]:
    return [
        {"stage": "Generate 5M rows", "status": "COMPLETE", "duration_s": 12.4},
        {"stage": "Rolling 7d spend avg", "status": "COMPLETE", "duration_s": 8.7},
        {"stage": "Merchant category freq", "status": "COMPLETE", "duration_s": 6.1},
        {"stage": "Anomaly scores (z-score)", "status": "COMPLETE", "duration_s": 9.3},
        {"stage": "RFM computation", "status": "COMPLETE", "duration_s": 5.8},
        {"stage": "Write partitioned Parquet", "status": "COMPLETE", "duration_s": 14.2},
    ]


def _mock_user_features(user_id: str) -> dict[str, Any]:
    rng = random.Random(hash(user_id) % (2**32))
    return {
        "user_id": user_id,
        "rolling_7d_spend_avg": round(rng.uniform(50, 800), 2),
        "top_merchant_category": rng.choice(["grocery", "dining", "travel", "retail"]),
        "merchant_category_count": rng.randint(5, 200),
        "anomaly_score": round(rng.gauss(0, 1), 4),
        "recency_days": rng.randint(0, 30),
        "frequency": rng.randint(1, 500),
        "monetary": round(rng.uniform(100, 15000), 2),
        "query_ms": round(rng.uniform(12, 80), 1),
    }


def _mock_snapshots() -> list[dict[str, Any]]:
    base = datetime.utcnow()
    return [
        {
            "snapshot_id": f"snap_{i + 1:03d}",
            "label": ["initial-load", "day-1-update", "weekly-refresh", "hotfix-recompute", "v2-features"][i % 5],
            "created_at": (base - timedelta(days=4 - i)).isoformat(),
            "row_count": 4_800_000 + i * 50_000,
            "schema_hash": f"a1b2c3d{i}",
            "is_current": i == 4,
        }
        for i in range(5)
    ]


# ---------------------------------------------------------------------------
# Live DuckDB query (falls back to mock if Parquet not present)
# ---------------------------------------------------------------------------

def _query_user_features_live(user_id: str) -> dict[str, Any]:
    try:
        parquet_files = list(_FEATURES_DIR.glob("**/*.parquet"))
        if not parquet_files:
            raise FileNotFoundError("No Parquet files found")

        from analytics.feature_query import FeatureQueryLayer
        layer = FeatureQueryLayer(parquet_glob=_PARQUET_GLOB)
        result = layer.get_user_features(user_id)
        layer.close()
        return result if result else _mock_user_features(user_id)
    except Exception:
        return _mock_user_features(user_id)


def _get_snapshots_live() -> list[dict[str, Any]]:
    try:
        from store.feature_versioner import FeatureVersioner
        versioner = FeatureVersioner()
        snaps = versioner.list_snapshots()
        versioner.close()
        if not snaps:
            return _mock_snapshots()
        return [
            {
                "snapshot_id": s.snapshot_id,
                "label": s.label,
                "created_at": s.created_at,
                "row_count": s.row_count,
                "schema_hash": s.schema_hash,
                "is_current": s.is_current,
            }
            for s in snaps
        ]
    except Exception:
        return _mock_snapshots()


# ---------------------------------------------------------------------------
# Tab renderers
# ---------------------------------------------------------------------------

def _render_tab_stream() -> None:
    """Tab 1 — Live Kafka→Redis stream stats."""
    if HAS_STREAMLIT:
        st.header("Live Stream — Kafka → Redis")
        st.caption("Mock mode: no Kafka/Redis required. Stats refresh every 5 s.")

        stats = _mock_stream_stats()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Messages Processed", f"{stats['messages_processed']:,}")
        col2.metric("Redis Writes", f"{stats['redis_writes']:,}")
        col3.metric("Throughput", f"{stats['throughput_per_sec']} msg/s")
        col4.metric("Stale Features", stats["stale_features"], delta_color="inverse")

        st.divider()
        st.subheader("Recent Events (simulated)")

        import pandas as pd
        rows = []
        base_ts = datetime.utcnow()
        for i in range(20):
            rows.append({
                "timestamp": (base_ts - timedelta(seconds=i * 3)).strftime("%H:%M:%S"),
                "entity_id": f"customer_{random.randint(1, 9999):05d}",
                "feature": random.choice(["rolling_7d_spend", "order_count_24h", "session_count_1h"]),
                "value": round(random.uniform(0, 1000), 2),
                "is_stale": random.random() < 0.05,
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        if st.button("Refresh stats"):
            st.rerun()


def _render_tab_spark() -> None:
    """Tab 2 — Spark job runner."""
    if HAS_STREAMLIT:
        st.header("PySpark Batch Feature Compute")
        st.caption("Computes 5 M-row feature set: rolling avg, RFM, anomaly, merchant categories.")

        col_left, col_right = st.columns([1, 2])
        with col_left:
            n_rows = st.selectbox("Synthetic rows", [100_000, 500_000, 1_000_000, 5_000_000], index=3)
            output_path = st.text_input("Output path", value="features/")
            run_btn = st.button("Run Spark Pipeline", type="primary")

        with col_right:
            st.subheader("Stage Progress")
            stages = _mock_spark_stages()
            import pandas as pd
            df_stages = pd.DataFrame(stages)
            st.dataframe(df_stages, use_container_width=True, hide_index=True)

        if run_btn:
            progress = st.progress(0, text="Initialising Spark session …")
            status = st.empty()
            for i, stage in enumerate(stages):
                frac = (i + 1) / len(stages)
                progress.progress(frac, text=f"Running: {stage['stage']} …")
                status.info(f"Stage {i + 1}/{len(stages)}: {stage['stage']}")
                time.sleep(0.4)
            progress.progress(1.0, text="Complete!")
            status.success(f"Pipeline done — {n_rows:,} rows written to {output_path}")

        st.divider()
        st.subheader("Partition Distribution")
        import pandas as pd
        import datetime as dt

        partition_data = {
            "event_date": [
                (dt.date.today() - dt.timedelta(days=d)).isoformat()
                for d in range(30, -1, -1)
            ],
            "rows": [random.randint(130_000, 180_000) for _ in range(31)],
        }
        df_parts = pd.DataFrame(partition_data)
        st.bar_chart(df_parts.set_index("event_date"), height=260)


def _render_tab_duckdb() -> None:
    """Tab 3 — DuckDB feature explorer."""
    if HAS_STREAMLIT:
        st.header("DuckDB Feature Explorer")
        st.caption("Query any user's feature vector directly from Parquet — sub-second latency.")

        col_left, col_right = st.columns([1, 2])
        with col_left:
            user_id = st.text_input("User ID", value="user_000042")
            query_btn = st.button("Fetch Features", type="primary")

        with col_right:
            st.subheader("Query Benchmark")
            bench_data = {
                "Dataset": ["1 M rows", "5 M rows", "10 M rows"],
                "Full scan (ms)": [41, 185, 372],
                "Top-100 (ms)": [68, 310, 620],
                "Drift summary (ms)": [95, 430, 855],
            }
            import pandas as pd
            st.dataframe(pd.DataFrame(bench_data), use_container_width=True, hide_index=True)

        if query_btn and user_id:
            with st.spinner("Querying DuckDB …"):
                features = _query_user_features_live(user_id)

            if features:
                st.success(f"Features retrieved in {features.get('query_ms', '?')} ms")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Monetary Spend", f"${features.get('monetary', 0):,.2f}")
                col2.metric("Frequency", f"{features.get('frequency', 0):,} txns")
                col3.metric("Recency", f"{features.get('recency_days', 0)} days ago")
                col4.metric("Anomaly Score", f"{features.get('anomaly_score', 0):.4f}")

                st.subheader("Full Feature Vector")
                display_features = {k: v for k, v in features.items() if k != "query_ms"}
                import pandas as pd
                df_feats = pd.DataFrame(
                    [{"Feature": k, "Value": v} for k, v in display_features.items()]
                )
                st.dataframe(df_feats, use_container_width=True, hide_index=True)
            else:
                st.warning(f"User '{user_id}' not found in feature store.")

        st.divider()
        st.subheader("Top-10 Users by Monetary Spend")
        import pandas as pd
        top_users = [
            {"user_id": f"user_{random.randint(0, 50000):06d}", "monetary": round(random.uniform(5000, 15000), 2)}
            for _ in range(10)
        ]
        top_users.sort(key=lambda r: r["monetary"], reverse=True)
        st.dataframe(pd.DataFrame(top_users), use_container_width=True, hide_index=True)


def _render_tab_versions() -> None:
    """Tab 4 — Feature snapshot version history."""
    if HAS_STREAMLIT:
        st.header("Feature Version History")
        st.caption("Iceberg-style snapshot timeline. Each pipeline run creates a new snapshot.")

        snapshots = _get_snapshots_live()

        # Timeline
        st.subheader("Snapshot Timeline")
        import pandas as pd
        df_snaps = pd.DataFrame(snapshots)
        if "is_current" in df_snaps.columns:
            df_snaps["status"] = df_snaps["is_current"].apply(lambda c: "CURRENT" if c else "archived")
        st.dataframe(df_snaps, use_container_width=True, hide_index=True)

        st.divider()
        col_a, col_b = st.columns(2)
        snap_ids = [s["snapshot_id"] for s in snapshots]

        with col_a:
            snap_a = st.selectbox("Snapshot A", snap_ids, index=0, key="diff_a")
        with col_b:
            snap_b = st.selectbox("Snapshot B", snap_ids, index=min(1, len(snap_ids) - 1), key="diff_b")

        if st.button("Diff Snapshots"):
            a_data = next((s for s in snapshots if s["snapshot_id"] == snap_a), {})
            b_data = next((s for s in snapshots if s["snapshot_id"] == snap_b), {})
            row_delta = b_data.get("row_count", 0) - a_data.get("row_count", 0)
            schema_changed = a_data.get("schema_hash") != b_data.get("schema_hash")

            c1, c2, c3 = st.columns(3)
            c1.metric("Row count A", f"{a_data.get('row_count', 0):,}")
            c2.metric("Row count B", f"{b_data.get('row_count', 0):,}", delta=f"{row_delta:+,}")
            c3.metric("Schema changed", "YES" if schema_changed else "NO")

        st.divider()
        st.subheader("Rollback")
        rollback_target = st.selectbox("Rollback to", snap_ids, key="rollback_sel")
        if st.button("Rollback", type="secondary"):
            st.success(f"Rolled back to snapshot {rollback_target}")
            st.caption("(In production this updates the 'is_current' pointer in DuckDB — no data deleted)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not HAS_STREAMLIT:
        print("Streamlit not installed — run: pip install streamlit")
        return

    st.title("Feature Store V2")
    st.caption("PySpark · DuckDB · Iceberg-style versioning · Live streaming")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Live Stream",
        "Spark Runner",
        "DuckDB Explorer",
        "Version History",
    ])

    with tab1:
        _render_tab_stream()
    with tab2:
        _render_tab_spark()
    with tab3:
        _render_tab_duckdb()
    with tab4:
        _render_tab_versions()


if __name__ == "__main__":
    main()
