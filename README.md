# kafka-stream-feature-store

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22c55e)
![Tests](https://img.shields.io/badge/Tests-284%20passing-22c55e)
![Stack](https://img.shields.io/badge/Stack-Kafka%20%7C%20Redis%20%7C%20Spark%20%7C%20DuckDB-6366f1)

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/downloads/)
[![Kafka](https://img.shields.io/badge/kafka-3.x-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/redis-7-DC382D?logo=redis)](https://redis.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**A real-time streaming feature store that materializes Kafka feature events into Redis with TTL-based freshness guarantees, serves them over FastAPI with per-feature SLA monitoring, and adds an offline Spark → DuckDB → Iceberg-style batch layer for large-scale feature computation and versioning.**

The streaming path (V1) trades a 24-hour batch refresh for second-scale freshness; the batch path (V2) computes and versions features over millions of rows. Both paths are designed for sub-60-second freshness as an explicit SLA target, and the freshness window is enforced — not just advertised — through Redis TTLs and a background monitor.

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Upstream Systems"]
        OMS[Order Management]
        POS[Point-of-Sale]
        CRM[Customer Data Platform]
    end

    subgraph Kafka["Apache Kafka 3.x"]
        topic["features.raw<br/>(idempotent producer:<br/>acks=all, snappy, retries)"]
        dlq["dead-letter handling<br/>(malformed messages)"]
    end

    subgraph Stream["Streaming Path (V1)"]
        consumer["FeatureConsumer<br/>consumer group<br/>(consumer.py)"]
        redis[("Redis 7<br/>feature:{entity}:{name}<br/>TTL = 2x freshness")]
        monitor["FeatureMonitor<br/>background SLA thread<br/>(monitor.py)"]
    end

    subgraph Registry["Feature Registry"]
        postgres[("PostgreSQL 14<br/>feature_definitions<br/>expected_freshness_seconds")]
    end

    subgraph API["Serving Layer"]
        fastapi["FastAPI<br/>input-validated lookups<br/>(serving.py)"]
    end

    subgraph Batch["Batch Path (V2)"]
        spark["PySpark pipeline<br/>rolling spend / RFM /<br/>z-score anomaly<br/>(spark/feature_pipeline.py)"]
        parquet[("Partitioned Parquet<br/>features/**/*.parquet")]
        duck["DuckDB query layer<br/>(analytics/feature_query.py)"]
        versioner["Iceberg-style versioner<br/>commit / rollback / diff<br/>(store/feature_versioner.py)"]
        window["Windowed aggregation<br/>tumbling + sliding<br/>(stream/windowed_agg.py)"]
    end

    subgraph Consumers["Downstream"]
        ml["Demand Forecasting Model"]
        dash["Streamlit Dashboard<br/>(dashboard/app_v2.py)"]
    end

    OMS & POS & CRM -->|FeatureEvent JSON| topic
    topic -->|consume| consumer
    topic -.->|on parse failure| dlq
    consumer -->|write + TTL| redis
    consumer -->|resolve freshness| postgres
    monitor -->|SCAN| redis
    monitor -->|definitions| postgres
    fastapi -->|GET| redis
    fastapi -->|freshness SLA| postgres
    fastapi -->|stale count| monitor
    ml -->|GET /features/:id| fastapi

    spark --> parquet --> duck --> versioner
    parquet --> window
    duck --> dash
```

For a deeper component breakdown and data-flow walkthrough, see [docs/architecture.md](docs/architecture.md).

---

## How it works

### Streaming write path (Kafka → Redis)

1. **Produce.** Upstream systems publish a Pydantic-validated `FeatureEvent` to `features.raw`. The producer (`feature_store/producer.py`) is configured for reliability: `acks=all` (wait for all in-sync replicas), `enable.idempotence=True` (no duplicates on retry), `retries=5` with backoff, `compression.type=snappy`, and `linger.ms`/`batch.size` tuning for throughput.
2. **Consume + materialize.** A consumer group (`feature_store/consumer.py`) reads each event, validates the schema, and writes to Redis under `feature:{entity_id}:{feature_name}`. The **Kafka offset is committed only after the Redis write succeeds**, giving at-least-once delivery. Malformed messages are routed to dead-letter handling rather than crashing the loop.
3. **TTL-based freshness.** Each key is written with `TTL = 2 x expected_freshness_seconds` (resolved from the PostgreSQL registry, with a fallback). Stale data therefore self-expires before it can mislead a downstream model.

### Serving read path (API → Model)

The FastAPI layer (`feature_store/serving.py`) reads an entity's keys from Redis and returns each value alongside `age_seconds` and `is_stale`, computed against the registry's `expected_freshness_seconds`. Entity IDs and feature names are regex-validated on the way in (colons are explicitly rejected) to prevent Redis key-structure injection.

### Monitoring path

`FeatureMonitor` (`feature_store/monitor.py`) runs as a background thread on a configurable interval (`MONITOR_INTERVAL_SECONDS`, default 15s). It scans Redis with cursor-based `SCAN`, compares each feature's age against its SLA, and surfaces a live stale-feature count through `GET /health`.

### Batch path (V2)

For offline / bulk feature computation the project adds a second pipeline:

- **`spark/feature_pipeline.py`** — a PySpark job (local mode) that generates synthetic transaction data and computes rolling spend averages, merchant-category frequency, a z-score anomaly signal, and RFM features, writing **date-partitioned Parquet**.
- **`analytics/feature_query.py`** — a DuckDB query layer over the Parquet output (`get_user_features`, `get_top_k_users`, `feature_drift_summary`) with a built-in `benchmark()`.
- **`store/feature_versioner.py`** — Iceberg-style snapshot versioning backed by DuckDB + JSON manifests, exposing `commit`, `rollback`, `diff`, and `list_snapshots`.
- **`stream/windowed_agg.py`** — a dependency-free (heapq + deque) windowing engine supporting tumbling (1min / 5min / 1hr) and sliding windows, with a runnable throughput benchmark.

---

## Performance: design targets & reproducible benchmarks

This project sets explicit, **reproducible** performance targets rather than quoting fixed headline numbers. The benchmarks below are runnable on any machine and print their own measured results:

| Component | Design target | Reproduce with |
|-----------|---------------|----------------|
| Streaming freshness | sub-60s, SLA-enforced via Redis TTL | `expected_freshness_seconds` in the registry + TTL multiplier |
| Windowed aggregation | ≥ 100K events/sec, single core | `python stream/windowed_agg.py --events 500000` |
| DuckDB feature query | sub-second on millions of rows | `python analytics/feature_query.py --benchmark` |
| Spark batch compute | partitioned Parquet over 5M synthetic rows | `python spark/feature_pipeline.py --rows 5000000` |

`stream/windowed_agg.py` emits `events_per_second`, `elapsed_seconds`, and a `target_100k_eps` pass/fail flag; `analytics/feature_query.py --benchmark` prints per-query timings across trials. Numbers depend on hardware — run them locally to capture your own.

> Note: the freshness SLA (`expected_freshness_seconds`, default 45 in seed data) is a configured, monitor-enforced threshold, not a hardware-fixed measurement. The TTL and monitor exist specifically to *guarantee* the window rather than assume it.

### Screenshots

| | |
|---|---|
| System architecture | ![Architecture](docs/screenshots/architecture.png) |
| Feature freshness — SLA compliance | ![Feature Freshness](docs/screenshots/feature_freshness.png) |
| Write latency vs throughput | ![Throughput](docs/screenshots/throughput_chart.png) |
| PySpark pipeline — stage waterfall | ![Spark Pipeline](docs/screenshots/v2_spark_pipeline.png) |
| DuckDB benchmark — query time vs rows | ![DuckDB Benchmark](docs/screenshots/v2_duckdb_benchmark.png) |
| Feature drift across snapshots | ![Feature Drift](docs/screenshots/v2_feature_drift.png) |
| Windowed aggregation throughput | ![Windowed Agg](docs/screenshots/v2_windowed_agg.png) |

---

## Quickstart

### Full streaming stack (Docker)

```bash
git clone https://github.com/shaikn6/kafka-stream-feature-store.git
cd kafka-stream-feature-store

# Start Kafka + ZooKeeper + Schema Registry + Redis + Postgres + API + Consumer
docker compose up --build -d

# Push synthetic feature events
docker compose exec api python scripts/simulate_producer.py --events 1000 --rate 10

# Query features for an entity
curl http://localhost:8000/features/customer_00042 | python -m json.tool

# SLA health
curl http://localhost:8000/health | python -m json.tool
```

### Local development

```bash
make install-dev          # deps + pytest/cov/xdist
make test                 # full suite with coverage (fail-under 80)
make lint type-check      # ruff + mypy
make ci                   # lint + type-check + security + test
```

If you prefer raw commands:

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=feature_store --cov-report=term-missing
streamlit run dashboard/app_v2.py
```

### Batch / V2 path

```bash
python spark/feature_pipeline.py --rows 5000000   # generate partitioned Parquet
python analytics/feature_query.py --benchmark     # DuckDB query timings
python store/feature_versioner.py                 # create + inspect snapshots
python stream/windowed_agg.py --events 500000     # windowing throughput benchmark
```

---

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/features/{entity_id}` | All features for an entity with freshness metadata |
| `GET` | `/features/{entity_id}/{feature_name}` | Single feature value |
| `GET` | `/health` | Stale feature count / SLA summary |
| `GET` | `/registry` | List registered feature definitions |
| `POST` | `/registry` | Register a new feature definition |

```bash
curl -X POST http://localhost:8000/registry \
  -H "Content-Type: application/json" \
  -d '{
    "feature_name": "session_count_1h",
    "description": "Web sessions in the last hour",
    "owner": "web-analytics",
    "expected_freshness_seconds": 30,
    "value_type": "int"
  }'
```

---

## Testing

```bash
pytest tests/ -v --cov=feature_store --cov-report=term-missing
```

**284 tests** across producer, consumer, monitor, serving, registry, and the V2 modules (windowed aggregation, feature versioner, DuckDB query layer, Spark pipeline). Tests run without external infrastructure: **fakeredis** stands in for Redis and **pytest-mock** for Kafka, so the suite is fully hermetic. Coverage gate: 80% (`--cov-fail-under=80`).

---

## Tech stack

| Layer | Technology |
|-------|------------|
| Streaming | Apache Kafka 3.x, confluent-kafka, Redis 7 |
| Serving | FastAPI, Uvicorn, Pydantic v2 |
| Registry | PostgreSQL 14, SQLAlchemy 2.x, Alembic |
| Batch compute | Apache Spark 3.5 (local mode), PyArrow / Parquet |
| Query & versioning | DuckDB 0.10, Iceberg-style snapshots (JSON manifests) |
| Stream windowing | Pure Python (heapq + deque) |
| Dashboard | Streamlit |
| Observability | structlog, prometheus-client |
| Tooling | pytest, fakeredis, ruff, mypy, bandit, pip-audit, pre-commit |
| Containers | Docker Compose (Kafka, ZooKeeper, Schema Registry, MinIO, Redis, Postgres) |

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_FEATURE_TOPIC` | `features.raw` | Topic to produce/consume |
| `KAFKA_CONSUMER_GROUP` | `feature-store-consumer` | Consumer group ID |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis serving layer |
| `DATABASE_URL` | `postgresql://featurestore:featurestore@localhost:5432/featurestore` | PostgreSQL DSN |
| `MONITOR_INTERVAL_SECONDS` | `15` | SLA check frequency |

---

## License

MIT
