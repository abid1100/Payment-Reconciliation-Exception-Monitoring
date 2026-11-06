-- ====================================================================
-- Parameterized Read-Only Reporting Queries (Dashboard & Tests)
-- ====================================================================

-- 1. Get Latest Succeeded Run ID
-- name: get_latest_succeeded_run
SELECT run_id, run_sequence, as_of, finished_at
FROM raw.pipeline_runs
WHERE status = 'SUCCEEDED'
ORDER BY run_sequence DESC
LIMIT 1;

-- 2. Fetch Reconciliation Overview KPI Cards
-- name: get_overview_by_run
SELECT
    run_id,
    as_of,
    run_status,
    total_rules_evaluated,
    matched_count,
    pending_count,
    exception_count,
    total_exception_discrepancy_minor
FROM reporting.v_reconciliation_overview
WHERE run_id = %(run_id)s;

-- 3. Fetch Exception Queue
-- name: get_exceptions_by_run
SELECT
    result_id,
    run_id,
    rule_id,
    subject_type,
    subject_id,
    state,
    reason_code,
    expected_value,
    observed_value,
    difference_minor,
    due_at,
    evaluated_at,
    evidence
FROM reporting.v_exception_queue
WHERE run_id = %(run_id)s
ORDER BY result_id ASC;

-- 4. Fetch Transaction Lineage Detail
-- name: get_lineage_by_transaction
SELECT
    transaction_id,
    transaction_type,
    order_id,
    parent_payment_id,
    transaction_amount_minor,
    order_amount_minor,
    settlement_entry_id,
    payout_id,
    settled_gross_minor,
    settled_fee_minor,
    settled_net_minor,
    payout_summary_amount_minor,
    occurred_at,
    settled_at,
    issued_at
FROM reporting.v_transaction_lineage
WHERE transaction_id = %(transaction_id)s;

-- 5. Fetch Pipeline Health History
-- name: get_pipeline_health
SELECT
    run_id,
    run_sequence,
    status,
    as_of,
    config_version,
    rule_version,
    started_at,
    finished_at,
    duration_seconds,
    input_summary,
    error_summary
FROM reporting.v_pipeline_health
ORDER BY run_sequence DESC
LIMIT %(limit)s;
