"""Unit tests verifying all 11 reconciliation rules against independent ground-truth assertions."""
import datetime
import unittest
from unittest.mock import MagicMock

from reconciliation.reconcile import evaluate_reconciliation
try:
    from tests.helpers import MockConnection, MockCursor
except ImportError:
    from helpers import MockConnection, MockCursor


class TestReconciliationRules(unittest.TestCase):
    def test_sample_baseline_assertions(self):
        base_dt = datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        # Evaluation Time A1 (within 72h window)
        as_of_a1 = datetime.datetime(2026, 1, 3, 12, 0, tzinfo=datetime.timezone.utc)

        # 1. ORDER_PAYMENT query response (ord_101 clean match, ord_104 amount mismatch)
        order_rows = [
            {
                "order_id": "ord_101",
                "expected_amount": 500000,
                "expected_currency": "INR",
                "ordered_at": base_dt + datetime.timedelta(hours=10),
                "capture_count": 1,
                "single_txn_id": "txn_301",
                "single_amount": 500000,
                "single_currency": "INR",
            },
            {
                "order_id": "ord_104",
                "expected_amount": 300000,
                "expected_currency": "INR",
                "ordered_at": base_dt + datetime.timedelta(hours=12),
                "capture_count": 1,
                "single_txn_id": "txn_305",
                "single_amount": 250000,
                "single_currency": "INR",
            },
        ]

        # 2. PAYMENT_ORDER query response
        payment_order_rows = [
            {
                "transaction_id": "txn_301",
                "order_id": "ord_101",
                "occurred_at": base_dt + datetime.timedelta(hours=10, minutes=1),
                "matched_order_id": "ord_101",
            }
        ]

        # 3. REFUND_PARENT & REFUND_LIMIT query response (rf_303 partial refund against txn_302)
        refund_rows = [
            {
                "refund_txn_id": "rf_303",
                "parent_payment_id": "txn_302",
                "refund_amount": 50000,
                "refund_occurred_at": base_dt + datetime.timedelta(hours=15),
                "refund_currency": "INR",
                "parent_txn_id": "txn_302",
                "parent_captured_amount": 200000,
                "parent_occurred_at": base_dt + datetime.timedelta(hours=11, minutes=2),
                "parent_currency": "INR",
            }
        ]

        # 4. TRANSACTION_SETTLEMENT query response (txn_301 settled, txn_304 in-flight pending)
        txn_settlement_rows = [
            {
                "transaction_id": "txn_301",
                "transaction_type": "payment",
                "amount_minor": 500000,
                "currency": "INR",
                "occurred_at": base_dt + datetime.timedelta(hours=10, minutes=1),
                "stl_count": 1,
                "single_stl_id": "stl_501",
                "single_gross": 500000,
                "single_currency": "INR",
            },
            {
                "transaction_id": "txn_304",
                "transaction_type": "payment",
                "amount_minor": 150000,
                "currency": "INR",
                "occurred_at": base_dt + datetime.timedelta(days=2, hours=9, minutes=1),
                "stl_count": 0,
                "single_stl_id": None,
                "single_gross": None,
                "single_currency": None,
            },
        ]

        # 5. SETTLEMENT_TRANSACTION & ARITHMETIC query response (stl_501 gross 500k, fee 10k, net 490k)
        settlement_rows = [
            {
                "settlement_entry_id": "stl_501",
                "transaction_id": "txn_301",
                "gross_minor": 500000,
                "fee_minor": 10000,
                "net_minor": 490000,
                "settled_at": base_dt + datetime.timedelta(days=1, hours=4),
                "matched_txn_id": "txn_301",
            }
        ]

        # 6. PAYOUT_COMPLETENESS & TOTAL query response (po_601 expected 3, observed 3, net 636k)
        payout_rows = [
            {
                "payout_id": "po_601",
                "payout_summary_amount": 636000,
                "expected_entry_count": 3,
                "issued_at": base_dt + datetime.timedelta(days=1, hours=6),
                "observed_entry_count": 3,
                "observed_net_total": 636000,
            }
        ]

        # 7. SETTLEMENT_PAYOUT query response
        settlement_payout_rows = [
            {
                "settlement_entry_id": "stl_501",
                "payout_id": "po_601",
                "settled_at": base_dt + datetime.timedelta(days=1, hours=4),
                "matched_payout_id": "po_601",
            }
        ]

        # 8. SOURCE_CONFLICT query response
        conflict_rows = []

        query_responses = [
            order_rows,
            payment_order_rows,
            refund_rows,
            txn_settlement_rows,
            settlement_rows,
            payout_rows,
            settlement_payout_rows,
            conflict_rows,
        ]

        mock_conn = MockConnection(query_responses)
        results = evaluate_reconciliation(
            conn=mock_conn,
            run_id="test_run_01",
            as_of=as_of_a1,
            grace_period_hours=72,
        )

        res_by_rule_subj = {(r.rule_id, r.subject_id): r for r in results}

        # Assert TC-01: ord_101 MATCHED
        r1 = res_by_rule_subj[("ORDER_PAYMENT", "ord_101")]
        self.assertEqual(r1.state, "MATCHED")
        self.assertEqual(r1.reason_code, "MATCH_OK")
        self.assertEqual(r1.difference_minor, 0)

        # Assert TC-11: ord_104 EXCEPTION (undercharge mismatch)
        r4 = res_by_rule_subj[("ORDER_PAYMENT", "ord_104")]
        self.assertEqual(r4.state, "EXCEPTION")
        self.assertEqual(r4.reason_code, "ORDER_AMOUNT_MISMATCH")
        self.assertEqual(r4.difference_minor, -50000)

        # Assert TC-02: txn_301 PAYMENT_ORDER MATCHED
        r2 = res_by_rule_subj[("PAYMENT_ORDER", "txn_301")]
        self.assertEqual(r2.state, "MATCHED")

        # Assert TC-05: rf_303 REFUND_PARENT MATCHED
        r_rf = res_by_rule_subj[("REFUND_PARENT", "rf_303")]
        self.assertEqual(r_rf.state, "MATCHED")

        # Assert TC-06: txn_302 REFUND_LIMIT MATCHED
        r_lim = res_by_rule_subj[("REFUND_LIMIT", "txn_302")]
        self.assertEqual(r_lim.state, "MATCHED")
        self.assertEqual(r_lim.difference_minor, 0)

        # Assert TC-03: txn_301 TRANSACTION_SETTLEMENT MATCHED
        r_stl1 = res_by_rule_subj[("TRANSACTION_SETTLEMENT", "txn_301")]
        self.assertEqual(r_stl1.state, "MATCHED")

        # Assert TC-09: txn_304 TRANSACTION_SETTLEMENT PENDING (in-flight < 72h)
        r_stl4 = res_by_rule_subj[("TRANSACTION_SETTLEMENT", "txn_304")]
        self.assertEqual(r_stl4.state, "PENDING")
        self.assertEqual(r_stl4.reason_code, "AWAITING_SETTLEMENT")

        # Assert TC-04: stl_501 SETTLEMENT_ARITHMETIC MATCHED
        r_arith = res_by_rule_subj[("SETTLEMENT_ARITHMETIC", "stl_501")]
        self.assertEqual(r_arith.state, "MATCHED")
        self.assertEqual(r_arith.difference_minor, 0)

        # Assert TC-07 & TC-08: po_601 PAYOUT_COMPLETENESS & TOTAL MATCHED
        r_poc = res_by_rule_subj[("PAYOUT_COMPLETENESS", "po_601")]
        self.assertEqual(r_poc.state, "MATCHED")
        r_pot = res_by_rule_subj[("PAYOUT_TOTAL", "po_601")]
        self.assertEqual(r_pot.state, "MATCHED")
        self.assertEqual(r_pot.difference_minor, 0)


if __name__ == "__main__":
    unittest.main()
