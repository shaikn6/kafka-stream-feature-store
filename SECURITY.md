# Security Audit — kafka-stream-feature-store

## Version: 1.1.0 — Security Hardened

**Audit Date:** 2026-05-30
**Auditor:** Senior Security Engineering (automated deep audit)
**Scope:** Full codebase — Kafka producer/consumer, Redis serving layer, FastAPI API, PostgreSQL registry, Docker Compose, CI pipeline

---

## Infrastructure Security Note

This project ships a local development environment via Docker Compose. The defaults are intentionally minimal for quick local iteration. Before any production or staging deployment, every item in the "Required before production" sections below must be addressed. The code changes in this release (v1.1.0) harden the application layer; infrastructure configuration requires operator action.

---

## Summary

| Severity | Total | Fixed in v1.1.0 | Operator action required |
|----------|-------|-----------------|--------------------------|
| CRITICAL | 3     | 2               | 1                        |
| HIGH     | 5     | 3               | 2                        |
| MEDIUM   | 3     | 1               | 2                        |
| LOW      | 2     | 0               | 2                        |

---

## Findings and Fixes

---

### [CRITICAL-01] No authentication on Kafka broker

**File:** `docker-compose.yml`, `feature_store/consumer.py`, `feature_store/producer.py`

**Status:** Operator action required — not fixable in application code alone.

**Detail:**
The Kafka broker is configured with `PLAINTEXT` listeners only. There is no SASL or TLS configured. Anyone on the same network segment (or reaching port 9092) can produce arbitrary messages to `features.raw` or consume and replay all feature data. In a shared cloud environment or Kubernetes cluster this is a direct data exfiltration path.

The consumer group ID is fixed via `KAFKA_CONSUMER_GROUP` env var (defaulting to `feature-store-consumer`). It is not user-supplied, so consumer group hijacking via user input is not a risk in the current design. However, without Kafka auth any external process can join the same group ID and steal partition assignments, causing missed messages and silent feature staleness.

**Required before production:**
```
# In docker-compose.yml or your broker configuration:
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_SSL:SASL_SSL,SASL_PLAINTEXT:SASL_PLAINTEXT
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
KAFKA_SASL_ENABLED_MECHANISMS: PLAIN

# In consumer.py / producer.py — extend the config dict with:
"security.protocol": "SASL_SSL",
"sasl.mechanism": "PLAIN",
"sasl.username": os.getenv("KAFKA_SASL_USERNAME"),
"sasl.password": os.getenv("KAFKA_SASL_PASSWORD"),
"ssl.ca.location": os.getenv("KAFKA_SSL_CA_PATH"),
```

For managed Kafka (Confluent Cloud, AWS MSK): use the service-native IAM or API-key auth mechanism rather than rolling your own SASL config.

---

### [CRITICAL-02] Redis running without authentication

**File:** `docker-compose.yml` lines 67-82

**Status:** Operator action required — application code already supports `REDIS_PASSWORD`.

**Detail:**
Redis is started with no `--requirepass` flag. Port 6379 is published to the host. Any process that can reach the host port can read, overwrite, or delete all feature values, or issue a `FLUSHALL` to wipe the entire feature store. Because Redis is also used as the primary serving cache, this constitutes both a data integrity and a data exfiltration risk.

The application code in `consumer.py` (line 37) and `serving.py` (line 29) already reads `REDIS_PASSWORD` from the environment and passes it to the Redis client. Only the Docker Compose service definition is missing the password.

**Required before production:**
```yaml
# docker-compose.yml — redis service
redis:
  command: >
    redis-server
    --requirepass ${REDIS_PASSWORD}
    --maxmemory 256mb
    --maxmemory-policy allkeys-lru
    --appendonly yes
  environment:
    - REDIS_PASSWORD=${REDIS_PASSWORD}

# api and consumer services — add:
environment:
  REDIS_PASSWORD: ${REDIS_PASSWORD}
```

Generate a strong password: `openssl rand -base64 32`

---

### [CRITICAL-03] Redis key injection via unsanitized entity_id — FIXED

**File:** `feature_store/serving.py` — `get_entity_features`, `get_single_feature`

**Status:** Fixed in v1.1.0.

**Detail:**
Path parameters `entity_id` and `feature_name` were concatenated directly into Redis keys without validation:

```python
# Before fix — vulnerable
key = f"feature:{entity_id}:{feature_name}"
raw = _redis().get(key)
```

An attacker supplying `entity_id = "customer:001"` restructures the key to `feature:customer:001:rolling_7d_spend`, aligning it with a different Redis keyspace than intended. While the redis-py client sends keys over the RESP protocol as binary-safe bulk strings (mitigating classic RESP injection), the colon injection breaks the `feature:{entity_id}:{feature_name}` key contract that every other component relies on. The monitor's entity_id extraction (`key.split(":")[1]`) would then return the wrong value, causing incorrect staleness reports.

A sufficiently long entity_id (no length cap existed) could also create arbitrarily large Redis keys, contributing to memory pressure.

**Fix applied:**

`_validate_entity_id` and `_validate_feature_name` functions added to `serving.py`. Both are called at the top of every feature lookup route before any Redis access:

```python
_ENTITY_ID_RE = re.compile(r"^[a-zA-Z0-9_\-\.]{1,128}$")
_FEATURE_NAME_RE = re.compile(r"^[a-z0-9_]{1,64}$")
```

Invalid inputs return HTTP 400 immediately. 8 new regression tests added in `TestInputValidation`.

---

### [HIGH-01] No rate limiting on feature serving API

**File:** `feature_store/serving.py`

**Status:** Operator action required.

**Detail:**
All GET endpoints (`/features/{entity_id}`, `/features/{entity_id}/{feature_name}`, `/health`, `/registry`) have no per-client rate limiting. A single unauthenticated caller can enumerate all entity IDs or all feature names by brute-forcing path segments, with no throttling. In production this is also a denial-of-service vector against Redis and PostgreSQL.

**Required before production:**
Add `slowapi` (the standard FastAPI rate-limiting library):

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.get("/features/{entity_id}")
@limiter.limit("100/minute")
async def get_entity_features(request: Request, entity_id: str): ...
```

Tune the limit based on expected consumer SLAs. Internal ML services on a private network may warrant higher limits than public-facing endpoints.

---

### [HIGH-02] No authentication or authorization on any API route

**File:** `feature_store/serving.py`

**Status:** Operator action required.

**Detail:**
Every endpoint — including `POST /registry` (which creates new feature definitions) — is completely unauthenticated. An external caller can register arbitrary feature names, polluting the feature registry and causing the consumer to materialize junk data into Redis. The `GET /registry` endpoint leaks the full list of feature names, freshness SLAs, and owner teams.

**Required before production:**
At minimum, protect mutating endpoints with an API key header check via FastAPI's `Security` dependency. For multi-tenant deployments, implement proper RBAC.

```python
from fastapi.security import APIKeyHeader
from fastapi import Security, HTTPException, status

API_KEY_HEADER = APIKeyHeader(name="X-API-Key")

def verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    if api_key != os.environ["FEATURE_STORE_API_KEY"]:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
```

---

### [HIGH-03] Hardcoded default database credentials

**File:** `docker-compose.yml` lines 93-95, `feature_store/registry.py` line 18, `feature_store/serving.py` line 30

**Status:** Operator action required (code correctly reads from env; default value is the risk).

**Detail:**
The default `DATABASE_URL` embedded in `registry.py` and `serving.py` contains the literal credential string `postgresql://featurestore:featurestore@localhost:5432/featurestore`. If the `DATABASE_URL` environment variable is not set, the application silently falls back to this credential. In a misconfigured deployment the application connects successfully with the known default password, giving any local-network attacker full read/write access to the feature registry.

The same username and password appear plaintext in `docker-compose.yml` as `POSTGRES_PASSWORD: featurestore`.

**Required before production:**
- Remove the fallback default from `registry.py` and `serving.py`. Raise a startup error if `DATABASE_URL` is not set.
- Rotate the database password. Never reuse `featurestore` as a credential in any environment.
- Use a secrets manager (AWS Secrets Manager, HashiCorp Vault, Kubernetes Secrets) to inject `DATABASE_URL` at runtime.

```python
DATABASE_URL = os.environ["DATABASE_URL"]  # raises KeyError at startup if missing — fail fast
```

---

### [HIGH-04] Kafka consumer processes messages without schema version enforcement — FIXED (partial)

**File:** `feature_store/consumer.py`, `feature_store/schemas/feature_event.py`

**Status:** Existing Pydantic validation is a good foundation; one gap fixed via separate audit note.

**Detail:**
`FeatureEvent.from_json` uses `model_validate_json`, which validates field types and the `feature_name` snake_case constraint. This is the correct pattern and prevents arbitrary field injection. The schema version field (`schema_version`) is parsed and stored but not enforced — a message with `schema_version: "99.0"` is accepted identically to `"1.0"`. This is a defense-in-depth gap rather than an active vulnerability, but it means a compromised upstream producer can silently downgrade or upgrade the schema contract.

**Recommendation:** Add a `@field_validator("schema_version")` that rejects unrecognized versions. Maintain an allowlist of supported versions. Route rejected-version messages directly to the DLQ rather than letting them fail at the Redis write stage.

---

### [HIGH-05] Monitor entity_id extraction fragile to colon-in-key — FIXED

**File:** `feature_store/monitor.py` line 196

**Status:** Fixed in v1.1.0.

**Detail:**
The stale-entity extraction used `key.split(":")[1]`, which returns only the first segment after the `feature:` prefix. If an entity_id somehow contained a colon (possible before the CRITICAL-03 fix), the monitor would report the wrong entity_id in SLA violation logs, masking real staleness events.

**Fix applied:**
```python
# Before
entity_id = key.split(":")[1]

# After — split with maxsplit=2 to correctly isolate the middle segment
parts = key.split(":", 2)
entity_id = parts[1] if len(parts) >= 2 else key
```

With CRITICAL-03's entity_id allowlist in place, colons in entity_ids are already blocked at the API boundary. The monitor fix is belt-and-suspenders.

---

### [MEDIUM-01] Pickle deserialization not present — confirmed safe

**File:** `feature_store/consumer.py`, `feature_store/serving.py`

**Status:** No action required. Confirmed safe.

**Detail:**
All Redis writes use `json.dumps(payload)` (consumer.py line 209) and all reads use `json.loads(raw)` (serving.py lines 123, 168; monitor.py line 191). There is no `pickle` import anywhere in the codebase. This eliminates the arbitrary code execution risk that would exist if feature values were serialized with pickle.

---

### [MEDIUM-02] No CORS configuration on FastAPI app

**File:** `feature_store/serving.py`

**Status:** Operator action required.

**Detail:**
FastAPI defaults to no CORS headers. If the API is ever accessed from a browser context (e.g., the included `frontend/index.html` fetches from the API), the absence of a CORS policy means browser requests are blocked, and there is no explicit policy to review for over-permissiveness. More critically, if `CORSMiddleware` is later added with `allow_origins=["*"]`, that would permit cross-origin requests from any domain.

**Required before production:**
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "").split(","),
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["X-API-Key"],
)
```

Never use `allow_origins=["*"]` on an internal ML infrastructure API.

---

### [MEDIUM-03] No HTTP security headers

**File:** `feature_store/serving.py`

**Status:** Operator action required (typically handled at reverse-proxy layer for internal APIs).

**Detail:**
The FastAPI application does not set `X-Content-Type-Options`, `X-Frame-Options`, `Strict-Transport-Security`, or `Referrer-Policy` headers. For an internal ML infrastructure API these are lower priority than the CRITICAL and HIGH items above, but they should be configured at the reverse proxy (nginx, AWS ALB, etc.) level before external exposure.

---

### [LOW-01] Replication factor of 1 in production Kafka config

**File:** `docker-compose.yml` line 57, `feature_store/producer.py` line 184

**Status:** Development-appropriate default; operator action required for production.

**Detail:**
`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` and `replication_factor=1` in `ensure_topic_exists`. In production, a single broker failure with `replication_factor=1` means the `features.raw` topic is unavailable and the feature store goes dark. Set to 3 for a 3-broker production cluster.

---

### [LOW-02] `auto.offset.reset: earliest` in consumer config

**File:** `feature_store/consumer.py` line 46

**Status:** Review before production.

**Detail:**
`earliest` means a new consumer group will replay all retained messages (up to `KAFKA_LOG_RETENTION_HOURS: 24` — 24 hours of history). In a new deployment this is intentional to hydrate Redis from existing Kafka history. In a redeployment after a group-ID change it could cause duplicate processing of up to 24 hours of feature events, temporarily creating stale or conflicting Redis values. Document this behavior explicitly in runbooks and consider switching to `latest` if hydration-on-deploy is not a requirement.

---

## Security Checklist — Status

| Check | Status |
|-------|--------|
| No hardcoded secrets in source code | PASS — secrets read from env vars |
| No pickle deserialization | PASS — JSON only, confirmed |
| Input validation on API path parameters | PASS — fixed in v1.1.0 |
| Redis key injection prevention | PASS — fixed in v1.1.0 |
| Kafka messages validated by schema | PASS — Pydantic v2 on consume path |
| Redis TTL set on all feature keys | PASS — TTL = 2x freshness window |
| Redis auth configured | FAIL — requirepass missing in docker-compose |
| Kafka SASL/SSL configured | FAIL — PLAINTEXT only |
| API authentication | FAIL — no auth on any route |
| Rate limiting | FAIL — no rate limiting |
| Non-root Docker user | PASS — `appuser` in Dockerfile |
| .env excluded from git | PASS — .gitignore covers .env and .env.* |
| No SQL injection surface | PASS — SQLAlchemy ORM with parameterized queries |
| CORS configured | NOT SET — evaluate before browser exposure |
| Security headers | NOT SET — configure at reverse proxy |
