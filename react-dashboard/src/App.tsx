import React, { useState, useEffect, useCallback } from "react";
import axios from "axios";
import FeatureCard from "./components/FeatureCard";
import LatencyChart from "./components/LatencyChart";
import { RedisStatusBadge, KafkaStatusBadge } from "./components/StatusBadge";
import { DashboardState, FeatureGroup, MetricPoint, KafkaConsumerLag } from "./types";
import "./styles/dashboard.css";

// ---------------------------------------------------------------------------
// Mock data — replaces API call when backend is unavailable
// ---------------------------------------------------------------------------
const FEATURE_GROUPS: FeatureGroup[] = [
  {
    id: "fg-user-behavior",
    name: "user_behavior_v3",
    description: "Real-time user click, session, and engagement signals aggregated from Kafka stream",
    featureCount: 24_580,
    onlineServingEnabled: true,
    lastMaterializationTime: new Date(Date.now() - 12_000).toISOString(),
    avgFreshnessMs: 4_200,
    staleFeaturesCount: 3,
    tags: { team: "recommendations", sla: "5s", env: "prod" },
  },
  {
    id: "fg-transaction",
    name: "transaction_features_v2",
    description: "Transaction-level fraud and spending pattern features, 7-day rolling window",
    featureCount: 18_940,
    onlineServingEnabled: true,
    lastMaterializationTime: new Date(Date.now() - 45_000).toISOString(),
    avgFreshnessMs: 38_000,
    staleFeaturesCount: 42,
    tags: { team: "risk", sla: "60s", env: "prod" },
  },
  {
    id: "fg-content",
    name: "content_embeddings_v1",
    description: "Pre-computed content embedding vectors and topic affinity scores",
    featureCount: 8_210,
    onlineServingEnabled: false,
    lastMaterializationTime: new Date(Date.now() - 3_600_000).toISOString(),
    avgFreshnessMs: 3_600_000,
    staleFeaturesCount: 8_210,
    tags: { team: "ml-platform", sla: "1h", env: "prod" },
  },
];

const generateLatencyHistory = (): MetricPoint[] => {
  const now = Date.now();
  return Array.from({ length: 60 }, (_, i) => ({
    timestamp: now - (59 - i) * 1_000,
    value: Math.round(2.1 + Math.sin(i / 8) * 0.8 + Math.random() * 0.6),
  }));
};

const generateConsumerLag = (): KafkaConsumerLag[] => [
  {
    consumerGroup: "feature-materializer-prod",
    topic: "user-events",
    partition: 0,
    lag: 1_204,
    lastCommittedOffset: 8_921_044,
    logEndOffset: 8_922_248,
  },
  {
    consumerGroup: "feature-materializer-prod",
    topic: "transaction-events",
    partition: 0,
    lag: 88,
    lastCommittedOffset: 4_501_092,
    logEndOffset: 4_501_180,
  },
];

const buildMockState = (): DashboardState => {
  const latencyHistory = generateLatencyHistory();
  const p50 = Math.round(latencyHistory[latencyHistory.length - 1].value);
  return {
    featureGroups: FEATURE_GROUPS,
    totalFeatureCount: FEATURE_GROUPS.reduce((s, g) => s + g.featureCount, 0),
    staleFeatureCount: FEATURE_GROUPS.reduce((s, g) => s + g.staleFeaturesCount, 0),
    latency: {
      p50Ms: p50,
      p95Ms: Math.round(p50 * 1.8),
      p99Ms: Math.round(p50 * 3.2),
      history: latencyHistory,
    },
    redis: {
      connected: true,
      host: "redis-prod.internal",
      port: 6379,
      memoryUsedBytes: 1_342_177_280,  // ~1.25 GB
      memoryMaxBytes: 4_294_967_296,   // 4 GB
      connectedClients: 24,
      uptimeSeconds: 1_209_600,
      latencyMs: 0.42,
    },
    kafka: {
      connected: true,
      brokerCount: 3,
      consumerLag: generateConsumerLag(),
      totalLag: 1_292,
      messagesPerSecond: 14_820,
    },
    lastRefreshed: new Date().toISOString(),
  };
};

// ---------------------------------------------------------------------------
// Custom hook — polls the API and falls back to mock data
// ---------------------------------------------------------------------------
const REFRESH_INTERVAL_MS = 5_000;

function useDashboard(): {
  state: DashboardState | null;
  loading: boolean;
  error: string | null;
} {
  const [state, setState] = useState<DashboardState | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchState = useCallback(async () => {
    try {
      const res = await axios.get<DashboardState>("/api/v1/dashboard/state", {
        timeout: 4_000,
      });
      setState(res.data);
      setError(null);
    } catch {
      // Fall back to mock data when the backend isn't running locally
      setState((prev) => {
        const fresh = buildMockState();
        if (!prev) return fresh;
        // Advance the latency history by one point
        const newHistory = [
          ...prev.latency.history.slice(1),
          {
            timestamp: Date.now(),
            value: Math.round(2.1 + Math.random() * 1.4),
          },
        ];
        return {
          ...fresh,
          latency: { ...fresh.latency, history: newHistory },
          lastRefreshed: new Date().toISOString(),
        };
      });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchState();
    const id = setInterval(fetchState, REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, [fetchState]);

  return { state, loading, error };
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------
const App: React.FC = () => {
  const { state, loading } = useDashboard();

  if (loading) {
    return (
      <div className="dashboard">
        <div className="dashboard__loading">Loading feature store metrics...</div>
      </div>
    );
  }

  if (!state) {
    return (
      <div className="dashboard">
        <div className="dashboard__error">Failed to load dashboard state.</div>
      </div>
    );
  }

  const stalePercent =
    state.totalFeatureCount > 0
      ? ((state.staleFeatureCount / state.totalFeatureCount) * 100).toFixed(1)
      : "0.0";

  return (
    <main className="dashboard">
      {/* Header */}
      <header className="dashboard__header">
        <div>
          <h1 className="dashboard__title">Feature Store Monitor</h1>
          <p className="dashboard__subtitle">kafka-stream-feature-store · prod cluster</p>
        </div>
        <span className="dashboard__last-refreshed">
          Refreshed {new Date(state.lastRefreshed).toLocaleTimeString()}
        </span>
      </header>

      <div className="dashboard__body">
        {/* Summary stats */}
        <section className="stats-bar" aria-label="Summary statistics">
          <div className="stats-bar__item">
            <span className="stats-bar__label">Total Features</span>
            <span className="stats-bar__value">
              {(state.totalFeatureCount / 1_000).toFixed(1)}K
            </span>
          </div>
          <div className="stats-bar__item">
            <span className="stats-bar__label">Feature Groups</span>
            <span className="stats-bar__value">{state.featureGroups.length}</span>
          </div>
          <div className="stats-bar__item">
            <span className="stats-bar__label">Stale Features</span>
            <span
              className={`stats-bar__value ${
                state.staleFeatureCount > 100
                  ? "stats-bar__value--error"
                  : state.staleFeatureCount > 10
                  ? "stats-bar__value--warn"
                  : "stats-bar__value--ok"
              }`}
            >
              {state.staleFeatureCount.toLocaleString()}
            </span>
          </div>
          <div className="stats-bar__item">
            <span className="stats-bar__label">Stale %</span>
            <span
              className={`stats-bar__value ${
                parseFloat(stalePercent) > 5
                  ? "stats-bar__value--error"
                  : parseFloat(stalePercent) > 1
                  ? "stats-bar__value--warn"
                  : "stats-bar__value--ok"
              }`}
            >
              {stalePercent}%
            </span>
          </div>
          <div className="stats-bar__item">
            <span className="stats-bar__label">p50 Latency</span>
            <span className="stats-bar__value">{state.latency.p50Ms}ms</span>
          </div>
          <div className="stats-bar__item">
            <span className="stats-bar__label">Kafka Lag</span>
            <span
              className={`stats-bar__value ${
                state.kafka.totalLag > 100_000
                  ? "stats-bar__value--error"
                  : state.kafka.totalLag > 10_000
                  ? "stats-bar__value--warn"
                  : "stats-bar__value--ok"
              }`}
            >
              {state.kafka.totalLag.toLocaleString()}
            </span>
          </div>
        </section>

        {/* Connection status */}
        <section aria-label="System status" className="status-badges">
          <RedisStatusBadge redis={state.redis} />
          <KafkaStatusBadge kafka={state.kafka} />
        </section>

        {/* Latency chart */}
        <LatencyChart metrics={state.latency} />

        {/* Feature group cards */}
        <section aria-labelledby="feature-groups-heading">
          <h2 id="feature-groups-heading" className="dashboard__subtitle" style={{ marginBottom: "1rem" }}>
            Feature Groups
          </h2>
          <div className="feature-cards-grid">
            {state.featureGroups.map((group) => (
              <FeatureCard key={group.id} group={group} />
            ))}
          </div>
        </section>
      </div>
    </main>
  );
};

export default App;
