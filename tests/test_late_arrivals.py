"""Tests for delayed records, time-only reevaluation, and exact 72-hour grace boundaries."""
import datetime
import unittest

from reconciliation.reconcile import evaluate_reconciliation
try:
    from tests.helpers import MockConnection
except ImportError:
    from helpers import MockConnection


class TestGracePeriodBoundaries(unittest.TestCase):
    def test_exact_72_hour_boundary_transition(self):
        occurred_at = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
        deadline = occurred_at + datetime.timedelta(hours=72)  # 2026-01-04T12:00:00Z

        # Just before deadline (71 hours, 59 minutes, 59 seconds)
        as_of_before = deadline - datetime.timedelta(seconds=1)
        # Exactly at deadline
        as_of_exact = deadline

        txn_rows = [
            {
                "transaction_id": "txn_delayed_01",
                "transaction_type": "payment",
                "amount_minor": 100000,
                "currency": "INR",
                "occurred_at": occurred_at,
                "stl_count": 0,
                "single_stl_id": None,
                "single_gross": None,
                "single_currency": None,
            }
        ]

        # Case 1: Before deadline -> PENDING
        responses_before = [[], [], [], txn_rows, [], [], [], []]
        conn_before = MockConnection(responses_before)
        results_before = evaluate_reconciliation(
            conn=conn_before,
            run_id="run_before",
            as_of=as_of_before,
            grace_period_hours=72,
        )
        r_before = results_before[0]
        self.assertEqual(r_before.state, "PENDING")
        self.assertEqual(r_before.reason_code, "AWAITING_SETTLEMENT")

        # Case 2: At or after deadline -> EXCEPTION
        responses_exact = [[], [], [], txn_rows, [], [], [], []]
        conn_exact = MockConnection(responses_exact)
        results_exact = evaluate_reconciliation(
            conn=conn_exact,
            run_id="run_exact",
            as_of=as_of_exact,
            grace_period_hours=72,
        )
        r_exact = results_exact[0]
        self.assertEqual(r_exact.state, "EXCEPTION")
        self.assertEqual(r_exact.reason_code, "SETTLEMENT_OVERDUE")


class TestLateArrivalResolution(unittest.TestCase):
    def test_late_settlement_resolves_pending_status(self):
        occurred_at = datetime.datetime(2026, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)
        # Snapshot 2: Settlement arrives on day 3
        as_of_day3 = datetime.datetime(2026, 1, 3, 10, 0, tzinfo=datetime.timezone.utc)

        txn_rows_resolved = [
            {
                "transaction_id": "txn_late_01",
                "transaction_type": "payment",
                "amount_minor": 200000,
                "currency": "INR",
                "occurred_at": occurred_at,
                "stl_count": 1,
                "single_stl_id": "stl_late_01",
                "single_gross": 200000,
                "single_currency": "INR",
            }
        ]
        responses = [[], [], [], txn_rows_resolved, [], [], [], []]
        conn = MockConnection(responses)
        results = evaluate_reconciliation(
            conn=conn,
            run_id="run_resolved",
            as_of=as_of_day3,
            grace_period_hours=72,
        )
        self.assertEqual(results[0].state, "MATCHED")
        self.assertEqual(results[0].reason_code, "MATCH_OK")
        self.assertEqual(results[0].difference_minor, 0)


if __name__ == "__main__":
    unittest.main()
