-- init_db.sql — initialise PostgreSQL schema for the feature registry
-- Run automatically by Docker Compose via the postgres init volume mount.

CREATE TABLE IF NOT EXISTS feature_definitions (
    id                          SERIAL PRIMARY KEY,
    feature_name                VARCHAR(255) NOT NULL UNIQUE,
    description                 TEXT,
    owner                       VARCHAR(255) NOT NULL,
    expected_freshness_seconds  INTEGER NOT NULL DEFAULT 60,
    value_type                  VARCHAR(50)  NOT NULL DEFAULT 'float',
    is_active                   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                  TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    updated_at                  TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

-- Index to speed up active-only lookups
CREATE INDEX IF NOT EXISTS idx_feature_definitions_active
    ON feature_definitions (is_active, feature_name);

-- Trigger: keep updated_at current on any update
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = (NOW() AT TIME ZONE 'UTC');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS feature_definitions_updated_at ON feature_definitions;
CREATE TRIGGER feature_definitions_updated_at
    BEFORE UPDATE ON feature_definitions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Seed the five demand-forecasting features used by the simulation
INSERT INTO feature_definitions (feature_name, description, owner, expected_freshness_seconds, value_type)
VALUES
    ('rolling_7d_spend',     'Sum of customer spend over past 7 days',          'ml-platform', 45,  'float'),
    ('order_count_24h',      'Number of orders placed in the past 24 hours',    'ml-platform', 60,  'int'),
    ('avg_basket_size',      'Average basket size over last 30 orders',         'ml-platform', 90,  'float'),
    ('days_since_last_order','Days elapsed since most recent order',            'ml-platform', 120, 'float'),
    ('preferred_category',   'Most frequently purchased product category',      'ml-platform', 300, 'str')
ON CONFLICT (feature_name) DO NOTHING;
