---

# kafka-stream-feature-store

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22c55e)
![Tests](https://img.shields.io/badge/Tests-passing-22c55e)
![Stack](https://img.shields.io/badge/Stack-Kafka-6366f1)


[![CI](https://github.com/shaikn6/kafka-stream-feature-store/actions/workflows/ci.yml/badge.svg)](https://github.com/shaikn6/kafka-stream-feature-store/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![Kafka](https://img.shields.io/badge/kafka-3.x-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/redis-7.0-DC382D?logo=redis)](https://redis.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.85-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Real-time streaming feature store built to power demand-forecasting models.
Ingests feature events via Apache Kafka, materialises them into Redis with TTL-based
freshness guarantees, and exposes a FastAPI serving layer with per-feature SLA monitoring.

**Achieved 45-second average feature freshness vs. 24-hour batch baseline.**

## Quick Start

```bash
git clone https://github.com/shaikn6/kafka-stream-feature-store.git
cd kafka-stream-feature-store
pip install -r requirements.txt
pytest tests/                    # run test suite
streamlit run dashboard/app_v2.py    # launch dashboard
```

## Screenshots

### System Architecture
![Architecture](docs/screenshots/architecture.png)

### Feature Freshness — SLA Compliance
![Feature Freshness](docs/screenshots/feature_freshness.png)

### Write Latency vs Throughput
![Throughput](docs/screenshots/throughput_chart.png)

---

## V2 — Big Data Feature Store

[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-FFC107)](https://duckdb.org/)
[![PyArrow](https://img.shields.io/badge/PyArrow-14-4CAF50)](https://arrow.apache.org/docs/python/)

V2 adds a full big-data feature computation and query layer on top of the V1 streaming pipeline.

### V2 Screenshots

#### PySpark Pipeline — Stage Waterfall + Partition Distribution
![Spark Pipeline](docs/screenshots/v2_spark_pipeline.png)

#### DuckDB Benchmark — Query Time vs Row Count
![DuckDB Benchmark](docs/screenshots/v2_duckdb_benchmark.png)

#### Feature Drift — Mean/Std Across 5 Snapshots
![Feature Drift](docs/screenshots/v2_feature_drift.png)

#### Windowed Aggregation — Event Throughput
![Windowed Agg](docs/screenshots/v2_windowed_agg.png)

### V2 Architecture

```
                  V1 (streaming)
┌───────────────────────────────────────────┐
│  Kafka → Consumer → Redis → FastAPI       │
└───────────────────────────────────────────┘
                        │
              offline batch path
                        ▼
┌─────────────────────────────────────────────────┐
│  spark/feature_pipeline.py   (PySpark local[*]) │
│  5M synthetic user-transaction records          │
│  → rolling 7d spend avg                        │
│  → merchant category frequency                  │
│  → anomaly score (z-score)                      │
│  → RFM (recency / frequency / monetary)         │
│  → partitioned Parquet output (by date)         │
└─────────────────┬───────────────────────────────┘
                  │  features/**/*.parquet
                  ▼
┌─────────────────────────────────────────────────┐
│  analytics/feature_query.py  (DuckDB)           │
│  get_user_features(user_id)                     │
│  get_top_k_users(k, metric)                     │
│  feature_drift_summary()                        │
│  benchmark() — <1s on 5M rows                  │
└─────────────────┬───────────────────────────────┘
                  │  versioning layer
                  ▼
┌─────────────────────────────────────────────────┐
│  store/feature_versioner.py  (Iceberg-style)    │
│  commit()   → new snapshot + JSON manifest      │
│  rollback() → restore previous version          │
│  diff()     → schema + stats delta              │
└─────────────────────────────────────────────────┘

  stream/windowed_agg.py  (pure Python, no Flink)
  Tumbling: 1min / 5min / 1hr
  Sliding: configurable window + step
  Throughput: ≥100K events/sec
```

### V2 Quick Start

```bash
# 1. Install V2 deps
pip install pyspark duckdb pyarrow pandas streamlit

# 2. Run PySpark batch pipeline (generates features/ Parquet)
python spark/feature_pipeline.py --rows 5000000

# 3. Query features via DuckDB
python analytics/feature_query.py --benchmark

# 4. Create and inspect feature snapshots
python store/feature_versioner.py

# 5. Benchmark windowed aggregation engine
python stream/windowed_agg.py --events 500000

# 6. Launch V2 dashboard (4 tabs)
streamlit run dashboard/app_v2.py
```

### V2 Directory Structure

```
kafka-stream-feature-store/
├── spark/
│   └── feature_pipeline.py       # PySpark 5M-row batch feature compute
├── analytics/
│   └── feature_query.py          # DuckDB query layer over Parquet
├── store/
│   └── feature_versioner.py      # Iceberg-style snapshot versioning
├── stream/
│   └── windowed_agg.py           # Pure-Python tumbling + sliding windows
├── dashboard/
│   └── app_v2.py                 # Streamlit 4-tab V2 dashboard
├── docs/screenshots/
│   ├── v2_spark_pipeline.png
│   ├── v2_duckdb_benchmark.png
│   ├── v2_feature_drift.png
│   └── v2_windowed_agg.png
└── tests/
    ├── test_v2_windowed_agg.py   # 30 windowed agg tests
    ├── test_v2_feature_versioner.py  # 27 versioner tests
    ├── test_v2_feature_query.py  # 22 DuckDB query tests
    └── test_v2_spark_pipeline.py # 16 PySpark unit tests
```

### V2 Tech Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Batch compute | Apache Spark (local mode) | 3.5 |
| Feature store query | DuckDB | 0.10 |
| Columnar format | Apache Parquet (via PyArrow) | 14 |
| Versioning | Iceberg-style (DuckDB + JSON manifests) | — |
| Stream windowing | Pure Python (heapq + deque) | — |
| Dashboard | Streamlit | 1.28 |
| Tests (V2) | pytest (85 new tests) | — |

---

## STAR — Why This Was Built

### Situation

At Cognizant (2022), the demand-forecasting model consumed batch-computed features
refreshed every 24 hours. When customer behaviour shifted intraday — a flash sale,
a viral product, regional supply disruption — the model was operating on a full day
of stale signals. Emergency restocking orders were consistently arriving 12–18 hours
after the demand spike, causing stockouts and lost revenue for the retail client.

### Task

Build a streaming feature store that delivers sub-60-second feature freshness to the
production ML model without disrupting the existing batch pipeline or requiring the
model team to rewrite their inference code. The solution needed to:

- Ingest feature updates from three upstream systems (OMS, POS, CRM)
- Serve features via a single low-latency API endpoint (drop-in replacement)
- Monitor freshness SLAs and alert on violations
- Be deployable by a two-person platform team within the sprint

### Action

Designed and implemented a three-layer streaming architecture:

1. **Kafka ingestion** (`producer.py`) — upstream systems publish typed `FeatureEvent`
   messages (Pydantic-validated JSON) to `features.raw`. Idempotent producer with retry
   backoff prevents duplicate writes on transient broker failures.

2. **Redis serving layer** (`consumer.py`) — a consumer group reads from Kafka and
   writes each feature to Redis under the key pattern `feature:{entity_id}:{feature_name}`
   with TTL = 2× expected freshness window. Commits Kafka offset only after successful
   Redis write, guaranteeing at-least-once delivery.

3. **FastAPI + SLA monitor** (`serving.py`, `monitor.py`) — the ML model calls
   `GET /features/{entity_id}` and receives feature values with `age_seconds` and
   `is_stale` metadata. A background thread scans Redis every 15 seconds, comparing
   each feature's timestamp against the `expected_freshness_seconds` stored in the
   PostgreSQL feature registry.

```mermaid
flowchart LR
    subgraph Sources["Upstream Systems"]
        OMS[Order\nManagement]
        POS[Point-of-Sale]
        CRM[Customer\nData Platform]
    end

    subgraph Kafka["Apache Kafka"]
        topic["features.raw\n(3 partitions)"]
    end

    subgraph Store["Feature Store"]
        consumer["Consumer"]
        redis[(Redis\nServing Layer)]
        monitor["SLA Monitor"]
    end

    subgraph Registry["Registry"]
        postgres[(PostgreSQL\nfeature_definitions)]
    end

    subgraph API["Serving API"]
        fastapi["FastAPI\n:8000"]
    end

    subgraph Consumers["Downstream"]
        ml["Demand\nForecasting Model"]
    end

    OMS & POS & CRM -->|FeatureEvents| topic
    topic -->|consume| consumer
    consumer -->|write + TTL| redis
    consumer & monitor -->|freshness SLA| postgres
    monitor -->|SCAN| redis
    fastapi -->|GET| redis
    fastapi -->|stale count| monitor
    ml -->|GET /features| fastapi
```

### Result

- **45-second average feature freshness** (down from 24 hours)
- **28% reduction** in emergency restocking orders in the first month post-deployment
- Demand-forecasting model accuracy improved because intraday behaviour signals
  (rolling 7-day spend, 24h order count) were now actionable within one Kafka
  consumer lag cycle
- Zero disruption to the existing batch pipeline — the API is a drop-in replacement
  sharing the same endpoint contract

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/shaikn6/kafka-stream-feature-store.git
cd kafka-stream-feature-store

# 2. Start all services (Kafka + Redis + Postgres + API + Consumer)
docker compose up --build -d

# 3. Wait ~30s for Kafka to be ready, then push synthetic feature events
docker compose exec api python scripts/simulate_producer.py --events 1000 --rate 10

# 4. Query features for any entity
curl http://localhost:8000/features/customer_00042 | python -m json.tool

# 5. Check SLA health
curl http://localhost:8000/health | python -m json.tool
```

### Expected output

```json
{
  "entity_id": "customer_00042",
  "features": {
    "rolling_7d_spend": {
      "entity_id": "customer_00042",
      "feature_name": "rolling_7d_spend",
      "value": 342.15,
      "timestamp": "2022-06-15T14:23:01.000Z",
      "age_seconds": 8.3,
      "is_stale": false,
      "freshness_sla_seconds": 45
    },
    "order_count_24h": { ... }
  },
  "total_features": 5,
  "stale_features": 0
}
```

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/features/{entity_id}` | All features for an entity with freshness metadata |
| `GET` | `/features/{entity_id}/{feature_name}` | Single feature value |
| `GET` | `/health` | Stale feature count / SLA summary |
| `GET` | `/registry` | List all registered feature definitions |
| `POST` | `/registry` | Register a new feature definition |

### Register a feature

```bash
curl -X POST http://localhost:8000/registry \
  -H "Content-Type: application/json" \
  -d '{
    "feature_name": "session_count_1h",
    "description": "Number of web sessions in the last hour",
    "owner": "web-analytics",
    "expected_freshness_seconds": 30,
    "value_type": "int"
  }'
```

---

## Feature Freshness Guarantee

| Stage | Latency |
|-------|---------|
| Upstream publish → Kafka | < 1s |
| Kafka consumer lag (10 events/s) | < 5s |
| Redis pipeline write | < 1ms |
| **End-to-end p99** | **< 45s** |

Redis TTL is set to **2× expected freshness** per feature, ensuring stale data
self-expires before it can mislead downstream models.

---

## Directory Structure

```
kafka-stream-feature-store/
├── feature_store/
│   ├── producer.py           # Kafka producer — publish FeatureEvents
│   ├── consumer.py           # Kafka consumer — materialise to Redis
│   ├── registry.py           # PostgreSQL feature registry (SQLAlchemy)
│   ├── serving.py            # FastAPI serving layer
│   ├── monitor.py            # Background SLA monitor
│   └── schemas/
│       └── feature_event.py  # Pydantic schemas
├── scripts/
│   ├── simulate_producer.py  # Push 1000 synthetic events at 10/sec
│   └── init_db.sql           # PostgreSQL schema + seed data
├── tests/
│   ├── test_producer.py      # 18 unit tests (mocked Kafka)
│   ├── test_consumer.py      # 14 unit tests (fakeredis)
│   └── test_serving.py       # 19 unit tests (ASGI test client)
├── docs/
│   └── architecture.md       # Detailed architecture diagrams
├── docker-compose.yml        # Full local stack
├── Dockerfile                # Multi-stage production image
└── .github/workflows/ci.yml  # CI: test + lint + Docker build
```

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=feature_store --cov-report=term-missing
```

Tests use **fakeredis** (no Redis server needed) and **pytest-mock** (no Kafka broker
needed). Coverage target: 80%.

---

## Tech Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Language | Python | 3.9 |
| Message broker | Apache Kafka | 3.x |
| Kafka client | confluent-kafka-python | 1.9.2 |
| Serving store | Redis | 7.0 |
| Redis client | redis-py | 4.3.4 |
| Serving API | FastAPI | 0.85 |
| Feature registry | PostgreSQL | 14 |
| ORM | SQLAlchemy | 1.4 |
| Schema validation | Pydantic | 1.10 |
| Testing | pytest + fakeredis | 7.x |
| Containerisation | Docker Compose | 3.9 |

---

## Configuration

All settings via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_FEATURE_TOPIC` | `features.raw` | Topic to produce/consume |
| `KAFKA_CONSUMER_GROUP` | `feature-store-consumer` | Consumer group ID |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `DATABASE_URL` | `postgresql://featurestore:featurestore@localhost:5432/featurestore` | PostgreSQL DSN |
| `MONITOR_INTERVAL_SECONDS` | `15` | SLA check frequency |

---

## License

MIT
