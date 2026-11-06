"""Reconciliation business logic, timing state machine, and snapshot evaluation."""
from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field
from typing import Any

from reconciliation.contracts import parse_utc_timestamp

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationResult:
    run_id: str
    rule_id: str
    subject_type: str
    subject_id: str
    state: str  # MATCHED, PENDING, EXCEPTION
    reason_code: str
    expected_value: str | None = None
    observed_value: str | None = None
    difference_minor: int | None = None
    due_at: datetime.datetime | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    evaluated_at: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))

    def to_tuple(self) -> tuple:
        return (
            self.run_id,
            self.rule_id,
            self.subject_type,
            self.subject_id,
            self.state,
            self.reason_code,
            self.expected_value,
            self.observed_value,
            self.difference_minor,
            self.due_at,
            json.dumps(self.evidence),
            self.evaluated_at,
        )


def evaluate_reconciliation(
    conn: Any,
    run_id: str,
    as_of: datetime.datetime | str,
    grace_period_hours: int = 72,
    max_run_sequence: int = 999999999,
) -> list[ReconciliationResult]:
    """Execute all 11 reconciliation rules against canonical state up to max_run_sequence."""
    as_of_dt = parse_utc_timestamp(as_of, "as_of")
    grace_delta = datetime.timedelta(hours=grace_period_hours)
    results: list[ReconciliationResult] = []

    with conn.cursor() as cur:
        # -------------------------------------------------------------
        # Rule 1: ORDER_PAYMENT
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                o.order_id,
                o.amount_minor AS expected_amount,
                o.currency AS expected_currency,
                o.ordered_at,
                COUNT(t.transaction_id) AS capture_count,
                MAX(t.transaction_id) AS single_txn_id,
                MAX(t.amount_minor) AS single_amount,
                MAX(t.currency) AS single_currency
            FROM core.orders o
            LEFT JOIN core.transactions t
                ON t.order_id = o.order_id
               AND t.transaction_type = 'payment'
               AND t.loaded_run_sequence <= %s
            WHERE o.loaded_run_sequence <= %s
            GROUP BY o.order_id, o.amount_minor, o.currency, o.ordered_at;
            """,
            (max_run_sequence, max_run_sequence),
        )
        for row in cur.fetchall():
            order_id = row["order_id"]
            due_at = row["ordered_at"] + grace_delta
            count = row["capture_count"]

            if count == 0:
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "AWAITING_PAYMENT" if state == "PENDING" else "PAYMENT_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="ORDER_PAYMENT",
                        subject_type="order",
                        subject_id=order_id,
                        state=state,
                        reason_code=reason,
                        expected_value=str(row["expected_amount"]),
                        observed_value=None,
                        due_at=due_at,
                        evidence={"ordered_at": row["ordered_at"].isoformat()},
                        evaluated_at=as_of_dt,
                    )
                )
            elif count == 1:
                observed_amount = row["single_amount"]
                if observed_amount == row["expected_amount"] and row["single_currency"] == row["expected_currency"]:
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="ORDER_PAYMENT",
                            subject_type="order",
                            subject_id=order_id,
                            state="MATCHED",
                            reason_code="MATCH_OK",
                            expected_value=str(row["expected_amount"]),
                            observed_value=str(observed_amount),
                            difference_minor=0,
                            evidence={"transaction_id": row["single_txn_id"]},
                            evaluated_at=as_of_dt,
                        )
                    )
                else:
                    diff = observed_amount - row["expected_amount"]
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="ORDER_PAYMENT",
                            subject_type="order",
                            subject_id=order_id,
                            state="EXCEPTION",
                            reason_code="ORDER_AMOUNT_MISMATCH",
                            expected_value=str(row["expected_amount"]),
                            observed_value=str(observed_amount),
                            difference_minor=diff,
                            evidence={"transaction_id": row["single_txn_id"]},
                            evaluated_at=as_of_dt,
                        )
                    )
            else:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="ORDER_PAYMENT",
                        subject_type="order",
                        subject_id=order_id,
                        state="EXCEPTION",
                        reason_code="MULTIPLE_CAPTURES_DETECTED",
                        expected_value="1 capture",
                        observed_value=f"{count} captures",
                        evidence={"ordered_at": row["ordered_at"].isoformat()},
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 2: PAYMENT_ORDER
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                t.transaction_id,
                t.order_id,
                t.occurred_at,
                o.order_id AS matched_order_id
            FROM core.transactions t
            LEFT JOIN core.orders o
                ON o.order_id = t.order_id
               AND o.loaded_run_sequence <= %s
            WHERE t.transaction_type = 'payment'
              AND t.loaded_run_sequence <= %s;
            """,
            (max_run_sequence, max_run_sequence),
        )
        for row in cur.fetchall():
            txn_id = row["transaction_id"]
            if row["matched_order_id"]:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="PAYMENT_ORDER",
                        subject_type="transaction",
                        subject_id=txn_id,
                        state="MATCHED",
                        reason_code="MATCH_OK",
                        expected_value=row["order_id"],
                        observed_value=row["matched_order_id"],
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                due_at = row["occurred_at"] + grace_delta
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "AWAITING_ORDER" if state == "PENDING" else "ORDER_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="PAYMENT_ORDER",
                        subject_type="transaction",
                        subject_id=txn_id,
                        state=state,
                        reason_code=reason,
                        expected_value=row["order_id"],
                        observed_value=None,
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 3: REFUND_PARENT & Rule 4: REFUND_LIMIT
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                r.transaction_id AS refund_txn_id,
                r.parent_payment_id,
                r.amount_minor AS refund_amount,
                r.occurred_at AS refund_occurred_at,
                r.currency AS refund_currency,
                p.transaction_id AS parent_txn_id,
                p.amount_minor AS parent_captured_amount,
                p.occurred_at AS parent_occurred_at,
                p.currency AS parent_currency
            FROM core.transactions r
            LEFT JOIN core.transactions p
                ON p.transaction_id = r.parent_payment_id
               AND p.transaction_type = 'payment'
               AND p.loaded_run_sequence <= %s
            WHERE r.transaction_type = 'refund'
              AND r.loaded_run_sequence <= %s;
            """,
            (max_run_sequence, max_run_sequence),
        )
        parent_totals: dict[str, int] = {}
        parent_limits: dict[str, int] = {}

        for row in cur.fetchall():
            rf_id = row["refund_txn_id"]
            parent_id = row["parent_payment_id"]
            if not row["parent_txn_id"]:
                due_at = row["refund_occurred_at"] + grace_delta
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "AWAITING_PARENT_PAYMENT" if state == "PENDING" else "PARENT_PAYMENT_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="REFUND_PARENT",
                        subject_type="transaction",
                        subject_id=rf_id,
                        state=state,
                        reason_code=reason,
                        expected_value=parent_id,
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                if row["refund_occurred_at"] < row["parent_occurred_at"]:
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="REFUND_PARENT",
                            subject_type="transaction",
                            subject_id=rf_id,
                            state="EXCEPTION",
                            reason_code="REFUND_PRECEDES_PARENT",
                            evidence={"parent_id": parent_id},
                            evaluated_at=as_of_dt,
                        )
                    )
                else:
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="REFUND_PARENT",
                            subject_type="transaction",
                            subject_id=rf_id,
                            state="MATCHED",
                            reason_code="MATCH_OK",
                            expected_value=parent_id,
                            observed_value=parent_id,
                            evaluated_at=as_of_dt,
                        )
                    )
                parent_totals[parent_id] = parent_totals.get(parent_id, 0) + row["refund_amount"]
                parent_limits[parent_id] = row["parent_captured_amount"]

        # Evaluate REFUND_LIMIT for parents
        for p_id, total_refunded in parent_totals.items():
            cap = parent_limits[p_id]
            if total_refunded <= cap:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="REFUND_LIMIT",
                        subject_type="transaction",
                        subject_id=p_id,
                        state="MATCHED",
                        reason_code="MATCH_OK",
                        expected_value=str(cap),
                        observed_value=str(total_refunded),
                        difference_minor=0,
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="REFUND_LIMIT",
                        subject_type="transaction",
                        subject_id=p_id,
                        state="EXCEPTION",
                        reason_code="REFUND_EXCEEDS_CAPTURE",
                        expected_value=str(cap),
                        observed_value=str(total_refunded),
                        difference_minor=(total_refunded - cap),
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 5: TRANSACTION_SETTLEMENT
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                t.transaction_id,
                t.transaction_type,
                t.amount_minor,
                t.currency,
                t.occurred_at,
                COUNT(se.settlement_entry_id) AS stl_count,
                MAX(se.settlement_entry_id) AS single_stl_id,
                MAX(se.gross_minor) AS single_gross,
                MAX(se.currency) AS single_currency
            FROM core.transactions t
            LEFT JOIN core.settlement_entries se
                ON se.transaction_id = t.transaction_id
               AND se.loaded_run_sequence <= %s
            WHERE t.loaded_run_sequence <= %s
            GROUP BY t.transaction_id, t.transaction_type, t.amount_minor, t.currency, t.occurred_at;
            """,
            (max_run_sequence, max_run_sequence),
        )
        for row in cur.fetchall():
            txn_id = row["transaction_id"]
            due_at = row["occurred_at"] + grace_delta
            count = row["stl_count"]

            expected_gross = row["amount_minor"] if row["transaction_type"] == "payment" else -row["amount_minor"]

            if count == 0:
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "AWAITING_SETTLEMENT" if state == "PENDING" else "SETTLEMENT_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="TRANSACTION_SETTLEMENT",
                        subject_type="transaction",
                        subject_id=txn_id,
                        state=state,
                        reason_code=reason,
                        expected_value=str(expected_gross),
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )
            elif count == 1:
                if row["single_gross"] == expected_gross:
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="TRANSACTION_SETTLEMENT",
                            subject_type="transaction",
                            subject_id=txn_id,
                            state="MATCHED",
                            reason_code="MATCH_OK",
                            expected_value=str(expected_gross),
                            observed_value=str(row["single_gross"]),
                            difference_minor=0,
                            evidence={"settlement_entry_id": row["single_stl_id"]},
                            evaluated_at=as_of_dt,
                        )
                    )
                else:
                    diff = row["single_gross"] - expected_gross
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="TRANSACTION_SETTLEMENT",
                            subject_type="transaction",
                            subject_id=txn_id,
                            state="EXCEPTION",
                            reason_code="SETTLEMENT_GROSS_MISMATCH",
                            expected_value=str(expected_gross),
                            observed_value=str(row["single_gross"]),
                            difference_minor=diff,
                            evidence={"settlement_entry_id": row["single_stl_id"]},
                            evaluated_at=as_of_dt,
                        )
                    )
            else:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="TRANSACTION_SETTLEMENT",
                        subject_type="transaction",
                        subject_id=txn_id,
                        state="EXCEPTION",
                        reason_code="DUPLICATE_SETTLEMENT_ENTRIES",
                        expected_value="1 line",
                        observed_value=f"{count} lines",
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 6: SETTLEMENT_TRANSACTION & Rule 7: SETTLEMENT_ARITHMETIC
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                se.settlement_entry_id,
                se.transaction_id,
                se.gross_minor,
                se.fee_minor,
                se.net_minor,
                se.settled_at,
                t.transaction_id AS matched_txn_id
            FROM core.settlement_entries se
            LEFT JOIN core.transactions t
                ON t.transaction_id = se.transaction_id
               AND t.loaded_run_sequence <= %s
            WHERE se.loaded_run_sequence <= %s;
            """,
            (max_run_sequence, max_run_sequence),
        )
        for row in cur.fetchall():
            stl_id = row["settlement_entry_id"]

            # Rule 6
            if row["matched_txn_id"]:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="SETTLEMENT_TRANSACTION",
                        subject_type="settlement_entry",
                        subject_id=stl_id,
                        state="MATCHED",
                        reason_code="MATCH_OK",
                        expected_value=row["transaction_id"],
                        observed_value=row["matched_txn_id"],
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                due_at = row["settled_at"] + grace_delta
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "ORPHAN_SETTLEMENT_AWAITING_TXN" if state == "PENDING" else "ORPHAN_SETTLEMENT_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="SETTLEMENT_TRANSACTION",
                        subject_type="settlement_entry",
                        subject_id=stl_id,
                        state=state,
                        reason_code=reason,
                        expected_value=row["transaction_id"],
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )

            # Rule 7: net = gross - fee
            expected_net = row["gross_minor"] - row["fee_minor"]
            if row["net_minor"] == expected_net:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="SETTLEMENT_ARITHMETIC",
                        subject_type="settlement_entry",
                        subject_id=stl_id,
                        state="MATCHED",
                        reason_code="MATCH_OK",
                        expected_value=str(expected_net),
                        observed_value=str(row["net_minor"]),
                        difference_minor=0,
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                diff = row["net_minor"] - expected_net
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="SETTLEMENT_ARITHMETIC",
                        subject_type="settlement_entry",
                        subject_id=stl_id,
                        state="EXCEPTION",
                        reason_code="SETTLEMENT_ARITHMETIC_ERROR",
                        expected_value=str(expected_net),
                        observed_value=str(row["net_minor"]),
                        difference_minor=diff,
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 8: PAYOUT_COMPLETENESS & Rule 9: PAYOUT_TOTAL
        # -------------------------------------------------------------
        cur.execute(
            """
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
               AND se.loaded_run_sequence <= %s
            WHERE p.loaded_run_sequence <= %s
            GROUP BY p.payout_id, p.amount_minor, p.expected_entry_count, p.issued_at;
            """,
            (max_run_sequence, max_run_sequence),
        )
        for row in cur.fetchall():
            p_id = row["payout_id"]
            due_at = row["issued_at"] + grace_delta
            obs_cnt = row["observed_entry_count"]
            exp_cnt = row["expected_entry_count"]

            if obs_cnt == exp_cnt:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="PAYOUT_COMPLETENESS",
                        subject_type="payout",
                        subject_id=p_id,
                        state="MATCHED",
                        reason_code="MATCH_OK",
                        expected_value=str(exp_cnt),
                        observed_value=str(obs_cnt),
                        evaluated_at=as_of_dt,
                    )
                )
                # Rule 9: PAYOUT_TOTAL
                if row["observed_net_total"] == row["payout_summary_amount"]:
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="PAYOUT_TOTAL",
                            subject_type="payout",
                            subject_id=p_id,
                            state="MATCHED",
                            reason_code="MATCH_OK",
                            expected_value=str(row["payout_summary_amount"]),
                            observed_value=str(row["observed_net_total"]),
                            difference_minor=0,
                            evaluated_at=as_of_dt,
                        )
                    )
                else:
                    diff = row["observed_net_total"] - row["payout_summary_amount"]
                    results.append(
                        ReconciliationResult(
                            run_id=run_id,
                            rule_id="PAYOUT_TOTAL",
                            subject_type="payout",
                            subject_id=p_id,
                            state="EXCEPTION",
                            reason_code="PAYOUT_TOTAL_MISMATCH",
                            expected_value=str(row["payout_summary_amount"]),
                            observed_value=str(row["observed_net_total"]),
                            difference_minor=diff,
                            evaluated_at=as_of_dt,
                        )
                    )
            elif obs_cnt < exp_cnt:
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "PAYOUT_LINES_INCOMPLETE" if state == "PENDING" else "PAYOUT_LINES_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="PAYOUT_COMPLETENESS",
                        subject_type="payout",
                        subject_id=p_id,
                        state=state,
                        reason_code=reason,
                        expected_value=str(exp_cnt),
                        observed_value=str(obs_cnt),
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="PAYOUT_TOTAL",
                        subject_type="payout",
                        subject_id=p_id,
                        state="PENDING",
                        reason_code="AWAITING_COMPLETE_LINES",
                        expected_value=str(row["payout_summary_amount"]),
                        observed_value=str(row["observed_net_total"]),
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="PAYOUT_COMPLETENESS",
                        subject_type="payout",
                        subject_id=p_id,
                        state="EXCEPTION",
                        reason_code="PAYOUT_OVERCOUNTED_LINES",
                        expected_value=str(exp_cnt),
                        observed_value=str(obs_cnt),
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 10: SETTLEMENT_PAYOUT
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                se.settlement_entry_id,
                se.payout_id,
                se.settled_at,
                p.payout_id AS matched_payout_id
            FROM core.settlement_entries se
            LEFT JOIN core.payouts p
                ON p.payout_id = se.payout_id
               AND p.loaded_run_sequence <= %s
            WHERE se.loaded_run_sequence <= %s;
            """,
            (max_run_sequence, max_run_sequence),
        )
        for row in cur.fetchall():
            stl_id = row["settlement_entry_id"]
            if row["matched_payout_id"]:
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="SETTLEMENT_PAYOUT",
                        subject_type="settlement_entry",
                        subject_id=stl_id,
                        state="MATCHED",
                        reason_code="MATCH_OK",
                        expected_value=row["payout_id"],
                        observed_value=row["matched_payout_id"],
                        evaluated_at=as_of_dt,
                    )
                )
            else:
                due_at = row["settled_at"] + grace_delta
                state = "PENDING" if as_of_dt < due_at else "EXCEPTION"
                reason = "ORPHAN_SETTLEMENT_AWAITING_PAYOUT" if state == "PENDING" else "ORPHAN_SETTLEMENT_PAYOUT_OVERDUE"
                results.append(
                    ReconciliationResult(
                        run_id=run_id,
                        rule_id="SETTLEMENT_PAYOUT",
                        subject_type="settlement_entry",
                        subject_id=stl_id,
                        state=state,
                        reason_code=reason,
                        expected_value=row["payout_id"],
                        due_at=due_at,
                        evaluated_at=as_of_dt,
                    )
                )

        # -------------------------------------------------------------
        # Rule 11: SOURCE_CONFLICT
        # -------------------------------------------------------------
        cur.execute(
            """
            SELECT
                canonical_id,
                source_type,
                rejection_reason
            FROM raw.raw_records
            WHERE parse_status = 'CONFLICT';
            """
        )
        for row in cur.fetchall():
            results.append(
                ReconciliationResult(
                    run_id=run_id,
                    rule_id="SOURCE_CONFLICT",
                    subject_type=row["source_type"],
                    subject_id=row["canonical_id"] or "UNKNOWN",
                    state="EXCEPTION",
                    reason_code="PAYLOAD_CONFLICT",
                    observed_value=row["rejection_reason"],
                    evaluated_at=as_of_dt,
                )
            )

    return results
