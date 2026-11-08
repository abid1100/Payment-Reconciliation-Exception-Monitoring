"""Read-only reporting queries shared by dashboard and tests."""
from __future__ import annotations

import json
from typing import Any


def get_latest_succeeded_run(conn: Any) -> dict[str, Any] | None:
    """Fetch the most recently completed SUCCEEDED run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, run_sequence, as_of, finished_at
            FROM reporting.v_pipeline_health
            WHERE status = 'SUCCEEDED'
            ORDER BY run_sequence DESC
            LIMIT 1;
            """
        )
        return cur.fetchone()


def get_reconciliation_overview(conn: Any, run_id: str) -> dict[str, Any] | None:
    """Fetch per-run reconciliation evaluation summary metrics (historical execution ledger).

    Note: exception_count here reflects exceptions newly evaluated in this specific run.
    For the cumulative operational exception queue that carries forward unresolved historical
    issues up to this run, see get_exception_queue().
    """
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
                exception_count
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
    """Fetch operational exception records visible as of the selected snapshot run.

    Exposes both:
    - selected_run_id: the snapshot cutoff run being inspected.
    - detected_run_id: the run that originally detected and recorded the exception.

    Integrates current-run detected exceptions with unresolved SOURCE_CONFLICT issues
    detected in prior SUCCEEDED runs up to selected_run_id, with deterministic aggregation
    of contributing record IDs, reasons, and detection provenance keyed by composite
    (source_type, subject_id).

    Validates that the selected run exists and has status = 'SUCCEEDED'.
    Operates strictly through approved reporting views (reporting.v_pipeline_health,
    reporting.v_exception_queue, reporting.v_unresolved_conflicts) so that restricted
    dashboard roles require no direct grants on raw.* tables.
    """
    selected_run_id = run_id

    with conn.cursor() as cur:
        # 1. Validate snapshot run status and obtain run_sequence strictly through reporting view
        cur.execute(
            """
            SELECT run_sequence, status
            FROM reporting.v_pipeline_health
            WHERE run_id = %s;
            """,
            (selected_run_id,),
        )
        health_row = cur.fetchone()

        if health_row is None:
            raise ValueError(f"Selected run '{selected_run_id}' not found")

        if isinstance(health_row, dict):
            status = health_row.get("status")
            selected_seq = health_row.get("run_sequence")
        else:
            selected_seq = health_row[0] if len(health_row) > 0 else None
            status = health_row[1] if len(health_row) > 1 else None

        # Fail closed: status must be explicitly 'SUCCEEDED'; missing/None status is rejected
        if status != "SUCCEEDED":
            raise ValueError(
                f"Selected run '{selected_run_id}' has status '{status}', expected 'SUCCEEDED'"
            )

        if selected_seq is None:
            raise ValueError(
                f"Selected run '{selected_run_id}' is missing run_sequence"
            )

        # 2. Fetch exceptions evaluated in the selected run
        query = """
            SELECT
                result_id,
                run_id AS detected_run_id,
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
        params = [selected_run_id]
        if rule_id:
            query += " AND rule_id = %s"
            params.append(rule_id)
        query += " ORDER BY result_id ASC;"

        cur.execute(query, tuple(params))
        current_rows = cur.fetchall() or []

        non_conflict_items: list[dict[str, Any]] = []
        # Keyed by (subject_type, subject_id)
        conflict_map: dict[tuple[str, str], dict[str, Any]] = {}

        for row in current_rows:
            d_row = dict(row) if isinstance(row, dict) else row
            r_id = d_row.get("rule_id")
            s_type = d_row.get("subject_type") or "unknown"
            s_id = d_row.get("subject_id") or "UNKNOWN"
            det_run_id = d_row.get("detected_run_id") or d_row.get("run_id") or selected_run_id

            if r_id == "SOURCE_CONFLICT":
                key = (s_type, s_id)
                ev = d_row.get("evidence") or {}
                if isinstance(ev, str):
                    try:
                        ev = json.loads(ev)
                    except Exception:
                        ev = {}
                rec_ids = set(ev.get("record_ids") or [])
                reasons = set()
                if d_row.get("observed_value"):
                    reasons.add(str(d_row["observed_value"]))
                if d_row.get("reason_code") and d_row["reason_code"] != "PAYLOAD_CONFLICT":
                    reasons.add(str(d_row["reason_code"]))

                conflict_map[key] = {
                    "result_id": d_row.get("result_id"),
                    "selected_run_id": selected_run_id,
                    "detected_run_id": det_run_id,
                    "detected_run_sequence": selected_seq or 0,
                    "source_type": s_type,
                    "subject_id": s_id,
                    "reasons": reasons,
                    "record_ids": rec_ids,
                    "detecting_run_ids": [det_run_id],
                    "evaluated_at": d_row.get("evaluated_at"),
                }
            else:
                d_row["selected_run_id"] = selected_run_id
                d_row["detected_run_id"] = det_run_id
                d_row["run_id"] = det_run_id
                non_conflict_items.append(d_row)

        # 3. If inspecting all rules or SOURCE_CONFLICT, carry forward prior unresolved conflicts
        # Query strictly through reporting.v_unresolved_conflicts (no raw fallback)
        if (rule_id is None or rule_id == "SOURCE_CONFLICT") and selected_seq is not None:
            cur.execute(
                """
                SELECT
                    disposition_id,
                    detected_run_id,
                    detected_run_sequence,
                    source_type,
                    subject_id,
                    rejection_reason,
                    record_id,
                    evaluated_at
                FROM reporting.v_unresolved_conflicts
                WHERE detected_run_sequence < %s
                ORDER BY detected_run_sequence ASC, disposition_id ASC;
                """,
                (selected_seq,),
            )
            prior_rows = cur.fetchall() or []

            for prow in prior_rows:
                pdict = dict(prow) if isinstance(prow, dict) else prow
                s_type = pdict.get("source_type") or "unknown"
                s_id = pdict.get("subject_id") or "UNKNOWN"
                key = (s_type, s_id)

                p_det_run = pdict.get("detected_run_id") or "unknown"
                p_det_seq = pdict.get("detected_run_sequence") or 0
                p_rec_id = pdict.get("record_id")
                p_reason = pdict.get("rejection_reason")
                p_eval_at = pdict.get("evaluated_at")

                if key in conflict_map:
                    entry = conflict_map[key]
                    if p_rec_id is not None:
                        entry["record_ids"].add(p_rec_id)
                    if p_reason:
                        entry["reasons"].add(p_reason)
                    if p_det_run not in entry["detecting_run_ids"]:
                        # Insert prior detecting run in chronological order
                        entry["detecting_run_ids"].append(p_det_run)

                    # Preserve original detector (lowest sequence / earliest run)
                    if p_det_seq < entry["detected_run_sequence"]:
                        entry["detected_run_id"] = p_det_run
                        entry["detected_run_sequence"] = p_det_seq
                        if p_eval_at:
                            entry["evaluated_at"] = p_eval_at
                else:
                    rec_set = {p_rec_id} if p_rec_id is not None else set()
                    reason_set = {p_reason} if p_reason else set()
                    conflict_map[key] = {
                        "result_id": f"op_conflict_{s_type}_{s_id}",
                        "selected_run_id": selected_run_id,
                        "detected_run_id": p_det_run,
                        "detected_run_sequence": p_det_seq,
                        "source_type": s_type,
                        "subject_id": s_id,
                        "reasons": reason_set,
                        "record_ids": rec_set,
                        "detecting_run_ids": [p_det_run],
                        "evaluated_at": p_eval_at,
                    }

        # 4. Assemble deterministic conflict items
        conflict_items: list[dict[str, Any]] = []
        for (s_type, s_id), agg in sorted(conflict_map.items()):
            obs = "; ".join(sorted(agg["reasons"])) if agg["reasons"] else "Conflicting business payload"
            rec_ids = sorted(list(agg["record_ids"]))
            det_runs = list(dict.fromkeys(agg["detecting_run_ids"]))

            conflict_items.append({
                "result_id": agg["result_id"],
                "selected_run_id": selected_run_id,
                "detected_run_id": agg["detected_run_id"],
                "run_id": agg["detected_run_id"],
                "rule_id": "SOURCE_CONFLICT",
                "subject_type": s_type,
                "subject_id": s_id,
                "state": "EXCEPTION",
                "reason_code": "PAYLOAD_CONFLICT",
                "expected_value": None,
                "observed_value": obs,
                "difference_minor": None,
                "due_at": None,
                "evaluated_at": agg.get("evaluated_at"),
                "evidence": {
                    "record_ids": rec_ids,
                    "detecting_run_ids": det_runs,
                    "contributing_run_ids": det_runs,
                },
            })

        return non_conflict_items + conflict_items


def get_transaction_lineage(conn: Any, transaction_id: str, run_id: str) -> list[dict[str, Any]]:
    """Return all settlement lines visible in the explicitly selected successful run."""
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
            WHERE transaction_id = %s AND run_id = %s
            ORDER BY settlement_entry_id NULLS FIRST;
            """,
            (transaction_id, run_id),
        )
        return cur.fetchall()


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
