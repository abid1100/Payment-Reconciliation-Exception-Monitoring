"""Read-only reporting queries shared by dashboard and tests."""
from __future__ import annotations

from typing import Any


def get_latest_succeeded_run(conn: Any) -> dict[str, Any] | None:
    """Fetch the most recently completed SUCCEEDED run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, run_sequence, as_of, finished_at
            FROM raw.pipeline_runs
            WHERE status = 'SUCCEEDED'
            ORDER BY run_sequence DESC
            LIMIT 1;
            """
        )
        return cur.fetchone()


def get_reconciliation_overview(conn: Any, run_id: str) -> dict[str, Any] | None:
    """Fetch KPI summary card metrics from reporting.v_reconciliation_overview."""
    with conn.cursor() as cur:
        cur.execute(
            """
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
            WHERE run_id = %s;
            """,
            (run_id,),
        )
        return cur.fetchone()


def get_exception_queue(
    conn: Any,
    run_id: str,
    rule_id: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch exception records for operational investigation."""
    query = """
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
        WHERE run_id = %s
    """
    params = [run_id]
    if rule_id:
        query += " AND rule_id = %s"
        params.append(rule_id)
    query += " ORDER BY result_id ASC;"

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        return cur.fetchall()


def get_transaction_lineage(conn: Any, transaction_id: str) -> dict[str, Any] | None:
    """Fetch 3-way reconciliation audit lineage for a specific transaction."""
    with conn.cursor() as cur:
        cur.execute(
            """
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
            WHERE transaction_id = %s;
            """,
            (transaction_id,),
        )
        return cur.fetchone()


def get_pipeline_health_history(conn: Any, limit: int = 10) -> list[dict[str, Any]]:
    """Fetch recent execution health and duration."""
    with conn.cursor() as cur:
        cur.execute(
            """
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
            LIMIT %s;
            """,
            (limit,),
        )
        return cur.fetchall()
