export type RiskLevel = "critical" | "high" | "medium" | "low" | "none";

export interface Feature {
  id: string;
  name: string;
  featureGroupId: string;
  valueType: "float" | "int" | "string" | "bool";
  lastUpdated: string; // ISO-8601
  freshnessMs: number;
  isStale: boolean;
  staleTtlMs: number;
}

export interface FeatureGroup {
  id: string;
  name: string;
  description: string;
  featureCount: number;
  onlineServingEnabled: boolean;
  lastMaterializationTime: string; // ISO-8601
  avgFreshnessMs: number;
  staleFeaturesCount: number;
  tags: Record<string, string>;
}

export interface MetricPoint {
  timestamp: number; // unix ms
  value: number;
  label?: string;
}

export interface LatencyMetrics {
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  history: MetricPoint[];
}

export interface KafkaConsumerLag {
  consumerGroup: string;
  topic: string;
  partition: number;
  lag: number;
  lastCommittedOffset: number;
  logEndOffset: number;
}

export interface RedisConnectionStatus {
  connected: boolean;
  host: string;
  port: number;
  memoryUsedBytes: number;
  memoryMaxBytes: number;
  connectedClients: number;
  uptimeSeconds: number;
  latencyMs: number;
}

export interface KafkaStatus {
  connected: boolean;
  brokerCount: number;
  consumerLag: KafkaConsumerLag[];
  totalLag: number;
  messagesPerSecond: number;
}

export interface DashboardState {
  featureGroups: FeatureGroup[];
  totalFeatureCount: number;
  staleFeatureCount: number;
  latency: LatencyMetrics;
  redis: RedisConnectionStatus;
  kafka: KafkaStatus;
  lastRefreshed: string;
}
