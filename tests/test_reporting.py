"""Tests for reporting queries and read-only dashboard layer."""
import unittest

from reconciliation.queries import (
    get_latest_succeeded_run,
    get_reconciliation_overview,
    get_exception_queue,
    get_transaction_lineage,
)


class MockCursor:
    def __init__(self, fetchone_val=None, fetchall_val=None):
        self.fetchone_val = fetchone_val
        self.fetchall_val = fetchall_val or []
        self.executed_queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        self.executed_queries.append((query, params))

    def fetchone(self):
        return self.fetchone_val

    def fetchall(self):
        return self.fetchall_val


class MockConn:
    def __init__(self, fetchone_val=None, fetchall_val=None):
        self.cur = MockCursor(fetchone_val, fetchall_val)

    def cursor(self):
        return self.cur


class TestReportingQueries(unittest.TestCase):
    def test_get_latest_succeeded_run(self):
        mock_run = {
            "run_id": "run_20260103_01",
            "run_sequence": 5,
            "as_of": "2026-01-03T12:00:00Z",
            "finished_at": "2026-01-03T12:05:00Z",
        }
        conn = MockConn(fetchone_val=mock_run)
        res = get_latest_succeeded_run(conn)
        self.assertEqual(res["run_id"], "run_20260103_01")
        self.assertEqual(res["run_sequence"], 5)
        self.assertIn("status = 'SUCCEEDED'", conn.cur.executed_queries[0][0])

    def test_get_reconciliation_overview(self):
        mock_overview = {
            "run_id": "run_20260103_01",
            "matched_count": 8,
            "pending_count": 1,
            "exception_count": 1,
            "total_exception_discrepancy_minor": 50000,
        }
        conn = MockConn(fetchone_val=mock_overview)
        res = get_reconciliation_overview(conn, "run_20260103_01")
        self.assertEqual(res["matched_count"], 8)
        self.assertEqual(res["total_exception_discrepancy_minor"], 50000)

    def test_get_exception_queue_with_rule_filter(self):
        mock_exceptions = [
            {
                "result_id": 1,
                "rule_id": "ORDER_PAYMENT",
                "subject_id": "ord_104",
                "state": "EXCEPTION",
                "reason_code": "ORDER_AMOUNT_MISMATCH",
                "difference_minor": -50000,
            }
        ]
        conn = MockConn(fetchall_val=mock_exceptions)
        res = get_exception_queue(conn, "run_20260103_01", rule_id="ORDER_PAYMENT")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["subject_id"], "ord_104")
        self.assertIn("AND rule_id = %s", conn.cur.executed_queries[0][0])


if __name__ == "__main__":
    unittest.main()
