-- ====================================================================
-- Reconciliation Analytical Queries (Set-Based Engine)
-- Parameters:
--   :as_of (TIMESTAMPTZ) - Evaluation anchor time
--   :grace_hours (INTEGER) - Configured grace period in hours (e.g. 72)
--   :max_run_seq (BIGINT) - Monotonic sequence cutoff for historical isolation
-- ====================================================================

-- 1. ORDER_PAYMENT Check
-- Identifies orders without captures, with amount mismatches, or with multiple captures
SELECT
    o.order_id,
    o.amount_minor AS order_amount,
    o.currency AS order_currency,
    o.ordered_at,
    COUNT(t.transaction_id) AS capture_count,
    MAX(t.transaction_id) AS single_txn_id,
    MAX(t.amount_minor) AS single_captured_amount,
    MAX(t.currency) AS single_captured_currency
FROM core.orders o
LEFT JOIN core.transactions t 
    ON t.order_id = o.order_id 
   AND t.transaction_type = 'payment'
   AND t.loaded_run_sequence <= :max_run_seq
WHERE o.loaded_run_sequence <= :max_run_seq
GROUP BY o.order_id, o.amount_minor, o.currency, o.ordered_at;

-- 2. PAYMENT_ORDER Check (Orphan Payment Captures)
SELECT
    t.transaction_id,
    t.order_id,
    t.amount_minor,
    t.occurred_at,
    o.order_id AS matched_order_id
FROM core.transactions t
LEFT JOIN core.orders o 
    ON o.order_id = t.order_id
   AND o.loaded_run_sequence <= :max_run_seq
WHERE t.transaction_type = 'payment'
  AND t.loaded_run_sequence <= :max_run_seq;

-- 3. REFUND_PARENT & REFUND_LIMIT Checks
SELECT
    r.transaction_id AS refund_txn_id,
    r.parent_payment_id,
    r.amount_minor AS refund_amount,
    r.occurred_at AS refund_occurred_at,
    p.transaction_id AS parent_txn_id,
    p.amount_minor AS parent_captured_amount,
    p.occurred_at AS parent_occurred_at,
    p.currency AS parent_currency,
    r.currency AS refund_currency,
    COALESCE(SUM(all_r.amount_minor), 0) AS total_refunded_to_parent
FROM core.transactions r
LEFT JOIN core.transactions p 
    ON p.transaction_id = r.parent_payment_id 
   AND p.transaction_type = 'payment'
   AND p.loaded_run_sequence <= :max_run_seq
LEFT JOIN core.transactions all_r
    ON all_r.parent_payment_id = r.parent_payment_id
   AND all_r.transaction_type = 'refund'
   AND all_r.loaded_run_sequence <= :max_run_seq
WHERE r.transaction_type = 'refund'
  AND r.loaded_run_sequence <= :max_run_seq
GROUP BY r.transaction_id, r.parent_payment_id, r.amount_minor, r.occurred_at,
         p.transaction_id, p.amount_minor, p.occurred_at, p.currency, r.currency;

-- 4. TRANSACTION_SETTLEMENT Check
SELECT
    t.transaction_id,
    t.transaction_type,
    t.amount_minor AS transaction_magnitude,
    t.currency AS transaction_currency,
    t.occurred_at,
    COUNT(se.settlement_entry_id) AS settlement_line_count,
    MAX(se.settlement_entry_id) AS single_stl_id,
    MAX(se.gross_minor) AS single_gross_minor,
    MAX(se.currency) AS single_stl_currency
FROM core.transactions t
LEFT JOIN core.settlement_entries se
    ON se.transaction_id = t.transaction_id
   AND se.loaded_run_sequence <= :max_run_seq
WHERE t.loaded_run_sequence <= :max_run_seq
GROUP BY t.transaction_id, t.transaction_type, t.amount_minor, t.currency, t.occurred_at;

-- 5. SETTLEMENT_ARITHMETIC & SETTLEMENT_TRANSACTION Checks
SELECT
    se.settlement_entry_id,
    se.payout_id,
    se.transaction_id,
    se.entry_type,
    se.gross_minor,
    se.fee_minor,
    se.net_minor,
    se.currency,
    se.settled_at,
    t.transaction_id AS matched_txn_id,
    t.transaction_type AS matched_txn_type
FROM core.settlement_entries se
LEFT JOIN core.transactions t
    ON t.transaction_id = se.transaction_id
   AND t.loaded_run_sequence <= :max_run_seq
WHERE se.loaded_run_sequence <= :max_run_seq;

-- 6. PAYOUT_COMPLETENESS & PAYOUT_TOTAL Checks
SELECT
    p.payout_id,
    p.amount_minor AS payout_summary_amount,
    p.expected_entry_count,
    p.issued_at,
    COUNT(se.settlement_entry_id) AS observed_entry_count,
    COALESCE(SUM(se.net_minor), 0) AS observed_net_total
FROM core.payouts p
LEFT JOIN core.settlement_entries se
    ON se.payout_id = p.payout_id
   AND se.loaded_run_sequence <= :max_run_seq
WHERE p.loaded_run_sequence <= :max_run_seq
GROUP BY p.payout_id, p.amount_minor, p.expected_entry_count, p.issued_at;
