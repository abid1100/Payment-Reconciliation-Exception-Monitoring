-- ====================================================================
-- Migration: 0001_initial.sql
-- Description: Initial schemas, tables, constraints, indexes, views and grants
-- ====================================================================

-- 1. Create Isolated Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS reporting;

-- 2. Schema: raw (Audit, Operational State & Ingestion Evidence)
CREATE TABLE IF NOT EXISTS raw.schema_migrations (
    version INTEGER PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    checksum CHAR(64) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS raw.source_batches (
    batch_id VARCHAR(64) PRIMARY KEY,
    manifest_hash CHAR(64) NOT NULL,
    schema_version VARCHAR(16) NOT NULL,
    delivered_at TIMESTAMPTZ NOT NULL,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    run_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'INTERRUPTED')),
    as_of TIMESTAMPTZ NOT NULL,
    config_version VARCHAR(32) NOT NULL,
    rule_version VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    finished_at TIMESTAMPTZ,
    input_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_summary TEXT
);

CREATE TABLE IF NOT EXISTS raw.raw_records (
    record_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL REFERENCES raw.source_batches(batch_id),
    source_type VARCHAR(32) NOT NULL,
    file_hash CHAR(64) NOT NULL,
    line_number INTEGER NOT NULL,
    raw_payload TEXT NOT NULL,
    parse_status VARCHAR(16) NOT NULL CHECK (parse_status IN ('ACCEPTED', 'DUPLICATE', 'CONFLICT', 'REJECTED')),
    rejection_reason TEXT,
    canonical_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
);
CREATE INDEX IF NOT EXISTS idx_raw_records_file_line ON raw.raw_records(file_hash, line_number);
CREATE INDEX IF NOT EXISTS idx_raw_records_canonical ON raw.raw_records(source_type, canonical_id);

-- 3. Schema: core (Canonical Deduplicated Business Facts)
CREATE TABLE IF NOT EXISTS core.orders (
    order_id VARCHAR(64) PRIMARY KEY,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    ordered_at TIMESTAMPTZ NOT NULL,
    loaded_run_sequence BIGINT NOT NULL,
    raw_record_id BIGINT NOT NULL REFERENCES raw.raw_records(record_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_ordered_at ON core.orders(ordered_at);

CREATE TABLE IF NOT EXISTS core.payment_events (
    event_id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('payment_captured', 'refund_succeeded')),
    transaction_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64),
    parent_payment_id VARCHAR(64),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    loaded_run_sequence BIGINT NOT NULL,
    raw_record_id BIGINT NOT NULL REFERENCES raw.raw_records(record_id)
);
CREATE INDEX IF NOT EXISTS idx_payment_events_txn ON core.payment_events(transaction_id);

CREATE TABLE IF NOT EXISTS core.transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    transaction_type VARCHAR(16) NOT NULL CHECK (transaction_type IN ('payment', 'refund')),
    order_id VARCHAR(64),
    parent_payment_id VARCHAR(64),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    occurred_at TIMESTAMPTZ NOT NULL,
    first_event_id VARCHAR(64) NOT NULL,
    loaded_run_sequence BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transactions_order ON core.transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_transactions_parent ON core.transactions(parent_payment_id);

CREATE TABLE IF NOT EXISTS core.settlement_entries (
    settlement_entry_id VARCHAR(64) PRIMARY KEY,
    payout_id VARCHAR(64) NOT NULL,
    transaction_id VARCHAR(64) NOT NULL,
    entry_type VARCHAR(16) NOT NULL CHECK (entry_type IN ('payment', 'refund')),
    gross_minor BIGINT NOT NULL,
    fee_minor BIGINT NOT NULL CHECK (fee_minor >= 0),
    net_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    settled_at TIMESTAMPTZ NOT NULL,
    loaded_run_sequence BIGINT NOT NULL,
    raw_record_id BIGINT NOT NULL REFERENCES raw.raw_records(record_id)
);
CREATE INDEX IF NOT EXISTS idx_settlement_txn ON core.settlement_entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_settlement_payout ON core.settlement_entries(payout_id);

CREATE TABLE IF NOT EXISTS core.payouts (
    payout_id VARCHAR(64) PRIMARY KEY,
    amount_minor BIGINT NOT NULL,
    expected_entry_count INTEGER NOT NULL CHECK (expected_entry_count >= 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    issued_at TIMESTAMPTZ NOT NULL,
    loaded_run_sequence BIGINT NOT NULL,
    raw_record_id BIGINT NOT NULL REFERENCES raw.raw_records(record_id)
);

-- 4. Schema: reporting (Reconciliation Results & Approved Views)
CREATE TABLE IF NOT EXISTS reporting.reconciliation_results (
    result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES raw.pipeline_runs(run_id),
    rule_id VARCHAR(32) NOT NULL,
    subject_type VARCHAR(32) NOT NULL,
    subject_id VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('MATCHED', 'PENDING', 'EXCEPTION')),
    reason_code VARCHAR(32) NOT NULL,
    expected_value TEXT,
    observed_value TEXT,
    difference_minor BIGINT,
    due_at TIMESTAMPTZ,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    evaluated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recon_results_run ON reporting.reconciliation_results(run_id);
CREATE INDEX IF NOT EXISTS idx_recon_results_state ON reporting.reconciliation_results(run_id, state);

-- 5. Approved Reporting Views (Owned by migration owner)
CREATE OR REPLACE VIEW reporting.v_reconciliation_overview AS
SELECT
    r.run_id,
    pr.as_of,
    pr.status AS run_status,
    COUNT(*) AS total_rules_evaluated,
    COUNT(*) FILTER (WHERE r.state = 'MATCHED') AS matched_count,
    COUNT(*) FILTER (WHERE r.state = 'PENDING') AS pending_count,
    COUNT(*) FILTER (WHERE r.state = 'EXCEPTION') AS exception_count,
    COALESCE(SUM(ABS(r.difference_minor)) FILTER (WHERE r.state = 'EXCEPTION'), 0) AS total_exception_discrepancy_minor
FROM reporting.reconciliation_results r
JOIN raw.pipeline_runs pr ON pr.run_id = r.run_id
GROUP BY r.run_id, pr.as_of, pr.status;

CREATE OR REPLACE VIEW reporting.v_exception_queue AS
SELECT
    r.result_id,
    r.run_id,
    r.rule_id,
    r.subject_type,
    r.subject_id,
    r.state,
    r.reason_code,
    r.expected_value,
    r.observed_value,
    r.difference_minor,
    r.due_at,
    r.evaluated_at,
    r.evidence
FROM reporting.reconciliation_results r
WHERE r.state = 'EXCEPTION';

CREATE OR REPLACE VIEW reporting.v_transaction_lineage AS
SELECT
    t.transaction_id,
    t.transaction_type,
    t.order_id,
    t.parent_payment_id,
    t.amount_minor AS transaction_amount_minor,
    o.amount_minor AS order_amount_minor,
    se.settlement_entry_id,
    se.payout_id,
    se.gross_minor AS settled_gross_minor,
    se.fee_minor AS settled_fee_minor,
    se.net_minor AS settled_net_minor,
    p.amount_minor AS payout_summary_amount_minor,
    t.occurred_at,
    se.settled_at,
    p.issued_at
FROM core.transactions t
LEFT JOIN core.orders o ON o.order_id = t.order_id
LEFT JOIN core.settlement_entries se ON se.transaction_id = t.transaction_id
LEFT JOIN core.payouts p ON p.payout_id = se.payout_id;

CREATE OR REPLACE VIEW reporting.v_pipeline_health AS
SELECT
    pr.run_id,
    pr.run_sequence,
    pr.status,
    pr.as_of,
    pr.config_version,
    pr.rule_version,
    pr.started_at,
    pr.finished_at,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(pr.finished_at, CLOCK_TIMESTAMP()) - pr.started_at))::numeric, 2) AS duration_seconds,
    pr.input_summary,
    pr.error_summary
FROM raw.pipeline_runs pr
ORDER BY pr.run_sequence DESC;

-- 6. Role Permissions & Explicit Least-Privilege Grants
DO $$
DECLARE
    pipe_role TEXT;
    dash_role TEXT;
BEGIN
    -- Apply grants to whichever pipeline role is present (development or isolated test)
    FOREACH pipe_role IN ARRAY ARRAY['recon_pipeline', 'recon_test_pipeline'] LOOP
        IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = pipe_role) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA raw, core, reporting TO %I', pipe_role);

            -- Canonical business facts: READ & INSERT ONLY (immutable ledger facts - no UPDATE, no DELETE)
            EXECUTE format('GRANT SELECT, INSERT ON core.orders, core.payment_events, core.transactions, core.settlement_entries, core.payouts TO %I', pipe_role);

            -- Reconciliation snapshots and raw provenance: READ & INSERT ONLY
            EXECUTE format('GRANT SELECT, INSERT ON reporting.reconciliation_results, raw.raw_records TO %I', pipe_role);

            -- Source batch tracking: INSERT & UPDATE only delivery timestamp
            EXECUTE format('GRANT SELECT, INSERT ON raw.source_batches TO %I', pipe_role);
            EXECUTE format('GRANT UPDATE (staged_at) ON raw.source_batches TO %I', pipe_role);

            -- Pipeline runs: INSERT & UPDATE only lifecycle status fields (never DELETE)
            EXECUTE format('GRANT SELECT, INSERT ON raw.pipeline_runs TO %I', pipe_role);
            EXECUTE format('GRANT UPDATE (status, finished_at, input_summary, error_summary) ON raw.pipeline_runs TO %I', pipe_role);

            -- Migration metadata: READ-ONLY inspection (cannot modify migration history)
            EXECUTE format('GRANT SELECT ON raw.schema_migrations TO %I', pipe_role);
            EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON raw.schema_migrations FROM %I', pipe_role);

            -- Sequences for auto-increment identity columns
            EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw, reporting TO %I', pipe_role);
        END IF;
    END LOOP;

    -- Apply grants to dashboard roles (development or test)
    FOREACH dash_role IN ARRAY ARRAY['recon_dashboard', 'recon_test_dashboard'] LOOP
        IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = dash_role) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA reporting TO %I', dash_role);

            -- Read access strictly to approved reporting views
            EXECUTE format('GRANT SELECT ON reporting.v_reconciliation_overview, reporting.v_exception_queue, reporting.v_transaction_lineage, reporting.v_pipeline_health TO %I', dash_role);

            -- Zero access to raw evidence and core transactional tables
            EXECUTE format('REVOKE ALL ON SCHEMA raw, core FROM %I', dash_role);
        END IF;
    END LOOP;
END $$;
