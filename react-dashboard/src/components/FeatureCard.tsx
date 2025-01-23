import React from "react";
import { FeatureGroup } from "../types";

interface FeatureCardProps {
  group: FeatureGroup;
}

const freshnessLabel = (avgMs: number): string => {
  if (avgMs < 1_000) return `${avgMs}ms`;
  if (avgMs < 60_000) return `${(avgMs / 1000).toFixed(1)}s`;
  if (avgMs < 3_600_000) return `${Math.round(avgMs / 60_000)}m`;
  return `${(avgMs / 3_600_000).toFixed(1)}h`;
};

const freshnessClass = (avgMs: number): string => {
  if (avgMs < 5_000) return "freshness--good";
  if (avgMs < 60_000) return "freshness--warn";
  return "freshness--stale";
};

const FeatureCard: React.FC<FeatureCardProps> = ({ group }) => {
  const lastMat = new Date(group.lastMaterializationTime);
  const staleRatio = group.featureCount > 0
    ? group.staleFeaturesCount / group.featureCount
    : 0;

  return (
    <article className="feature-card">
      <header className="feature-card__header">
        <h2 className="feature-card__name">{group.name}</h2>
        <span
          className={`feature-card__online-badge ${group.onlineServingEnabled ? "feature-card__online-badge--active" : "feature-card__online-badge--inactive"}`}
        >
          {group.onlineServingEnabled ? "Online" : "Offline"}
        </span>
      </header>

      <p className="feature-card__description">{group.description}</p>

      <dl className="feature-card__stats">
        <div className="feature-card__stat">
          <dt>Features</dt>
          <dd>{group.featureCount.toLocaleString()}</dd>
        </div>

        <div className="feature-card__stat">
          <dt>Avg Freshness</dt>
          <dd className={freshnessClass(group.avgFreshnessMs)}>
            {freshnessLabel(group.avgFreshnessMs)}
          </dd>
        </div>

        <div className="feature-card__stat">
          <dt>Stale Features</dt>
          <dd className={group.staleFeaturesCount > 0 ? "text-warn" : "text-ok"}>
            {group.staleFeaturesCount.toLocaleString()}
            <span className="feature-card__stat-pct">
              {" "}({(staleRatio * 100).toFixed(1)}%)
            </span>
          </dd>
        </div>

        <div className="feature-card__stat">
          <dt>Last Materialized</dt>
          <dd title={lastMat.toISOString()}>
            {lastMat.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
          </dd>
        </div>
      </dl>

      {group.tags && Object.keys(group.tags).length > 0 && (
        <footer className="feature-card__tags">
          {Object.entries(group.tags).map(([k, v]) => (
            <span key={k} className="feature-card__tag">
              {k}={v}
            </span>
          ))}
        </footer>
      )}
    </article>
  );
};

export default FeatureCard;
