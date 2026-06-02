import React from "react";
import { RedisConnectionStatus, KafkaStatus } from "../types";

interface RedisStatusBadgeProps {
  redis: RedisConnectionStatus;
}

interface KafkaStatusBadgeProps {
  kafka: KafkaStatus;
}

const memoryPercent = (used: number, max: number): number =>
  max > 0 ? Math.round((used / max) * 100) : 0;

export const RedisStatusBadge: React.FC<RedisStatusBadgeProps> = ({ redis }) => {
  const memPct = memoryPercent(redis.memoryUsedBytes, redis.memoryMaxBytes);
  const memWarning = memPct > 80;

  return (
    <div className={`status-badge ${redis.connected ? "status-badge--ok" : "status-badge--error"}`}>
      <span className="status-badge__dot" />
      <div className="status-badge__body">
        <span className="status-badge__label">
          Redis {redis.connected ? "Connected" : "Disconnected"}
        </span>
        {redis.connected && (
          <span className="status-badge__detail">
            {redis.host}:{redis.port} · {redis.connectedClients} clients · {redis.latencyMs}ms
          </span>
        )}
        {redis.connected && (
          <span className={`status-badge__detail ${memWarning ? "status-badge__detail--warn" : ""}`}>
            Memory {memPct}% ({formatBytes(redis.memoryUsedBytes)} / {formatBytes(redis.memoryMaxBytes)})
          </span>
        )}
      </div>
    </div>
  );
};

export const KafkaStatusBadge: React.FC<KafkaStatusBadgeProps> = ({ kafka }) => {
  const lagWarning = kafka.totalLag > 10_000;
  const lagCritical = kafka.totalLag > 100_000;
  const lagClass = lagCritical
    ? "status-badge__detail--error"
    : lagWarning
    ? "status-badge__detail--warn"
    : "";

  return (
    <div className={`status-badge ${kafka.connected ? "status-badge--ok" : "status-badge--error"}`}>
      <span className="status-badge__dot" />
      <div className="status-badge__body">
        <span className="status-badge__label">
          Kafka {kafka.connected ? "Connected" : "Disconnected"}
        </span>
        {kafka.connected && (
          <>
            <span className="status-badge__detail">
              {kafka.brokerCount} broker{kafka.brokerCount !== 1 ? "s" : ""} · {kafka.messagesPerSecond.toLocaleString()} msg/s
            </span>
            <span className={`status-badge__detail ${lagClass}`}>
              Consumer lag: {kafka.totalLag.toLocaleString()} msgs
            </span>
          </>
        )}
      </div>
    </div>
  );
};

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`;
}
