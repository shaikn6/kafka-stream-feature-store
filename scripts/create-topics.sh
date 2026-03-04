#!/usr/bin/env bash
# create-topics.sh — idempotent Kafka topic creation for feature-store
# Usage:
#   ./scripts/create-topics.sh [--bootstrap-server localhost:9092]
#   ./scripts/create-topics.sh --bootstrap-server kafka:29092 --replication-factor 3
#
# For production Strimzi clusters, prefer KafkaTopic CRDs (k8s/kafka-topics.yaml).
# This script is for local dev (docker-compose) and CI bootstrap.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVER:-localhost:9092}"
REPLICATION="${REPLICATION_FACTOR:-1}"     # 1 for local, 3 for prod
PARTITIONS_HIGH=24                          # raw-events (high throughput)
PARTITIONS_MED=12                           # feature-updates
PARTITIONS_LOW=6                            # DLQs and audit

log()  { printf "\e[32m[topics]\e[0m %s
" "$*"; }
warn() { printf "\e[33m[warn  ]\e[0m %s
" "$*" >&2; }
die()  { printf "\e[31m[error ]\e[0m %s
" "$*" >&2; exit 1; }

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP="$2"; shift 2 ;;
    --replication-factor) REPLICATION="$2"; shift 2 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

log "Bootstrap server : $BOOTSTRAP"
log "Replication factor: $REPLICATION"

# Verify kafka-topics.sh is available
KAFKA_TOPICS_CMD=""
for candidate in     kafka-topics.sh     /usr/bin/kafka-topics.sh     /opt/kafka/bin/kafka-topics.sh     /opt/bitnami/kafka/bin/kafka-topics.sh; do
  if command -v "$candidate" &>/dev/null; then
    KAFKA_TOPICS_CMD="$candidate"
    break
  fi
done

if [[ -z "$KAFKA_TOPICS_CMD" ]]; then
  # Try docker exec fallback for local dev
  if docker ps --format '{{.Names}}' | grep -q "feature-kafka"; then
    log "Using docker exec into feature-kafka container"
    KAFKA_TOPICS_CMD="docker exec feature-kafka kafka-topics"
    BOOTSTRAP="localhost:9092"
  else
    die "kafka-topics command not found. Install Kafka or ensure the broker container is running."
  fi
fi

# Wait for broker to be ready (up to 60s)
wait_for_broker() {
  local retries=30
  local delay=2
  log "Waiting for broker at $BOOTSTRAP ..."
  for i in $(seq 1 $retries); do
    if $KAFKA_TOPICS_CMD --bootstrap-server "$BOOTSTRAP" --list &>/dev/null; then
      log "Broker is ready."
      return 0
    fi
    warn "Attempt $i/$retries failed, retrying in ${delay}s..."
    sleep "$delay"
  done
  die "Broker at $BOOTSTRAP did not become ready within $((retries * delay))s"
}

# Create or validate a topic
create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="${3:-604800000}"    # default: 7 days
  local retention_bytes="${4:--1}"        # default: unlimited
  local cleanup_policy="${5:-delete}"
  local extra_configs="${6:-}"

  if $KAFKA_TOPICS_CMD --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" &>/dev/null; then
    log "Topic '$name' already exists — skipping."
    return 0
  fi

  log "Creating topic: $name (partitions=$partitions, replicas=$REPLICATION)"

  local args=(
    --bootstrap-server "$BOOTSTRAP"
    --create
    --topic "$name"
    --partitions "$partitions"
    --replication-factor "$REPLICATION"
    --config "retention.ms=$retention_ms"
    --config "retention.bytes=$retention_bytes"
    --config "cleanup.policy=$cleanup_policy"
    --config "compression.type=lz4"
    --config "min.insync.replicas=$(( REPLICATION > 1 ? 2 : 1 ))"
    --config "max.message.bytes=10485760"
  )

  if [[ -n "$extra_configs" ]]; then
    while IFS= read -r cfg; do
      args+=(--config "$cfg")
    done <<< "$extra_configs"
  fi

  $KAFKA_TOPICS_CMD "${args[@]}"
  log "  Created: $name"
}

wait_for_broker

log ""
log "=== Creating feature-store topics ==="

# raw-events — high-throughput inbound feature events
create_topic "raw-events"   "$PARTITIONS_HIGH"   "604800000"   "107374182400"   "delete"   "segment.bytes=1073741824
message.max.bytes=10485760"

# feature-updates — computed feature vectors for serving
create_topic "feature-updates"   "$PARTITIONS_MED"   "86400000"   "10737418240"   "delete"   "segment.bytes=536870912
compression.type=snappy"

# feature-updates-dlq — DLQ for failed feature computations
create_topic "feature-updates-dlq"   "$PARTITIONS_LOW"   "2592000000"   "5368709120"   "delete"   "compression.type=gzip"

# raw-events-dlq — DLQ for malformed inbound events
create_topic "raw-events-dlq"   "$PARTITIONS_LOW"   "2592000000"   "2147483648"   "delete"   "compression.type=gzip"

# feature-audit — compact+delete audit log
create_topic "feature-audit"   "$PARTITIONS_LOW"   "2592000000"   "-1"   "compact,delete"   "min.compaction.lag.ms=3600000
delete.retention.ms=86400000
compression.type=lz4"

log ""
log "=== Topic summary ==="
$KAFKA_TOPICS_CMD --bootstrap-server "$BOOTSTRAP" --list | grep -E "^(raw-events|feature-updates|feature-audit)" | sort
log ""
log "Done. All topics provisioned successfully."
