-- =============================================================
-- 01_create_schemas.sql
-- Creates all schemas and tables for the e-commerce platform
-- Runs automatically when PostgreSQL container starts
-- =============================================================

-- ── Schemas ───────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw_events;   -- PySpark writes here
CREATE SCHEMA IF NOT EXISTS analytics;   -- dbt writes here

-- ── raw_events.orders ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_events.orders (
    order_id         VARCHAR(30)    NOT NULL,
    customer_id      VARCHAR(20)    NOT NULL,
    product_id       VARCHAR(20),
    product_name     VARCHAR(150),
    category         VARCHAR(50),
    quantity         INTEGER,
    unit_price       DECIMAL(10,2),
    order_total      DECIMAL(10,2),
    order_status     VARCHAR(30),
    shipping_state   VARCHAR(10),
    payment_method   VARCHAR(30),
    event_type       VARCHAR(30),
    event_timestamp  TIMESTAMP,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,
    processed_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id   ON raw_events.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_event_ts      ON raw_events.orders(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_category      ON raw_events.orders(category);

-- ── raw_events.clickstream ────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_events.clickstream (
    event_id         VARCHAR(20)    NOT NULL,
    session_id       VARCHAR(30),
    customer_id      VARCHAR(20),
    event_type       VARCHAR(30),
    page             VARCHAR(200),
    device_type      VARCHAR(20),
    browser          VARCHAR(30),
    referrer         VARCHAR(50),
    product_id       VARCHAR(20),
    search_term      VARCHAR(200),
    results_count    INTEGER,
    cart_value       DECIMAL(10,2),
    event_timestamp  TIMESTAMP,
    processed_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clicks_session_id   ON raw_events.clickstream(session_id);
CREATE INDEX IF NOT EXISTS idx_clicks_event_ts     ON raw_events.clickstream(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_clicks_customer_id  ON raw_events.clickstream(customer_id);

-- ── raw_events.inventory_updates ──────────────────────────────
CREATE TABLE IF NOT EXISTS raw_events.inventory_updates (
    update_id        VARCHAR(20)    NOT NULL,
    product_id       VARCHAR(20),
    warehouse_id     VARCHAR(20),
    update_type      VARCHAR(20),
    quantity_change  INTEGER,
    stock_after      INTEGER,
    low_stock_alert  BOOLEAN,
    event_timestamp  TIMESTAMP,
    processed_at     TIMESTAMP DEFAULT NOW()
);

-- ── raw_events.order_window_aggregates ────────────────────────
-- Written by PySpark windowed streaming query
CREATE TABLE IF NOT EXISTS raw_events.order_window_aggregates (
    window_start       TIMESTAMP,
    window_end         TIMESTAMP,
    category           VARCHAR(50),
    shipping_state     VARCHAR(10),
    order_count        INTEGER,
    total_revenue      DECIMAL(12,2),
    avg_order_value    DECIMAL(10,2),
    total_items_sold   INTEGER,
    unique_customers   INTEGER,
    computed_at        TIMESTAMP DEFAULT NOW()
);

-- ── raw_events.funnel_window_metrics ──────────────────────────
CREATE TABLE IF NOT EXISTS raw_events.funnel_window_metrics (
    window_start         TIMESTAMP,
    window_end           TIMESTAMP,
    device_type          VARCHAR(20),
    referrer             VARCHAR(50),
    page_views           INTEGER,
    product_views        INTEGER,
    add_to_cart          INTEGER,
    checkout_starts      INTEGER,
    checkouts            INTEGER,
    unique_sessions      INTEGER,
    unique_users         INTEGER,
    conversion_rate_pct  DECIMAL(6,2),
    computed_at          TIMESTAMP DEFAULT NOW()
);

-- ── raw_events.hourly_revenue_by_category ────────────────────
CREATE TABLE IF NOT EXISTS raw_events.hourly_revenue_by_category (
    category           VARCHAR(50),
    shipping_state     VARCHAR(10),
    order_count        INTEGER,
    total_revenue      DECIMAL(12,2),
    avg_order_value    DECIMAL(10,2),
    total_items        INTEGER,
    unique_customers   INTEGER,
    processing_hour    VARCHAR(30),
    processed_at       TIMESTAMP DEFAULT NOW()
);

-- ── analytics schema placeholders (dbt creates the real tables) ──
-- These ensure the schema exists before dbt runs
CREATE TABLE IF NOT EXISTS analytics.dbt_placeholder (id SERIAL);

GRANT ALL PRIVILEGES ON SCHEMA raw_events TO admin;
GRANT ALL PRIVILEGES ON SCHEMA analytics  TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_events TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics  TO admin;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_events GRANT ALL ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics  GRANT ALL ON TABLES TO admin;
