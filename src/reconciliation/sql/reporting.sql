-- ====================================================================
-- Parameterized Read-Only Reporting Queries (Dashboard & Tests)
-- ====================================================================

-- 1. Get Latest Succeeded Run ID
-- name: get_latest_succeeded_run
SELECT run_id, run_sequence, as_of, finished_at
FROM reporting.v_pipeline_health
WHERE status = 'SUCCEEDED'
ORDER BY run_sequence DESC
LIMIT 1;

-- 2. Fetch Reconciliation Overview KPI Cards (Per-Run Evaluation Ledger)
-- name: get_overview_by_run
-- NOTE: exception_count here reflects exceptions newly evaluated in this specific run.
-- For cumulative outstanding operational issues carried forward across runs,
-- refer to the authoritative Python contract reconciliation.queries.get_exception_queue().
SELECT
    run_id,
    as_of,
    run_status,
    total_rules_evaluated,
    matched_count,
    pending_count,
    exception_count
FROM reporting.v_reconciliation_overview
WHERE run_id = %(run_id)s;

-- 3. Fetch Historical Per-Run Exceptions (Immutable Evaluation Results for Run)
-- name: get_exceptions_by_run
-- NOTE: This query returns only exceptions newly evaluated and recorded in this specific run.
-- It represents the immutable historical audit ledger for %(run_id)s.
-- The cumulative operational exception queue (which carries forward unresolved historical
-- conflicts across runs, aggregates contributing record IDs, reasons, and detection provenance
-- keyed by composite (source_type, subject_id)) is authoritatively implemented in Python:
-- reconciliation.queries.get_exception_queue(conn, run_id, rule_id=None).
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
WHERE transaction_id = %(transaction_id)s AND run_id = %(run_id)s
ORDER BY settlement_entry_id NULLS FIRST;

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
