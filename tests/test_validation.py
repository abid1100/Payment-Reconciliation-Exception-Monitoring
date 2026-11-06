"""Unit tests for strict contracts and data validation rules."""
import datetime
import unittest

from reconciliation.contracts import (
    OrderRecord,
    PaymentEventRecord,
    SettlementEntryRecord,
    PayoutRecord,
    BatchManifest,
    parse_integer_minor,
    parse_utc_timestamp,
    validate_currency,
)


class TestNumericValidation(unittest.TestCase):
    def test_accepts_valid_integers(self):
        self.assertEqual(parse_integer_minor(10000, "amount"), 10000)
        self.assertEqual(parse_integer_minor("500000", "amount"), 500000)
        self.assertEqual(parse_integer_minor(0, "amount"), 0)

    def test_rejects_floating_point(self):
        with self.assertRaises(ValueError):
            parse_integer_minor(100.5, "amount")

    def test_rejects_boolean(self):
        with self.assertRaises(ValueError):
            parse_integer_minor(True, "amount")
        with self.assertRaises(ValueError):
            parse_integer_minor(False, "amount")

    def test_rejects_fractional_string(self):
        with self.assertRaises(ValueError):
            parse_integer_minor("100.50", "amount")

    def test_negative_handling(self):
        with self.assertRaises(ValueError):
            parse_integer_minor(-500, "amount", allow_negative=False)
        self.assertEqual(parse_integer_minor(-500, "amount", allow_negative=True), -500)


class TestTimestampValidation(unittest.TestCase):
    def test_accepts_iso8601_utc(self):
        dt = parse_utc_timestamp("2026-01-01T10:00:00Z", "ts")
        self.assertEqual(dt.tzinfo, datetime.timezone.utc)
        self.assertEqual(dt.hour, 10)

    def test_normalizes_timezone_offsets(self):
        dt = parse_utc_timestamp("2026-01-01T15:30:00+05:30", "ts")
        self.assertEqual(dt.tzinfo, datetime.timezone.utc)
        self.assertEqual(dt.hour, 10)  # 15:30 - 05:30 = 10:00 UTC

    def test_rejects_naive_timestamp(self):
        with self.assertRaises(ValueError):
            parse_utc_timestamp("2026-01-01T10:00:00", "ts")

    def test_rejects_malformed_string(self):
        with self.assertRaises(ValueError):
            parse_utc_timestamp("not-a-timestamp", "ts")


class TestContractModels(unittest.TestCase):
    def test_valid_order_record(self):
        order = OrderRecord.from_dict({
            "order_id": "ord_001",
            "amount_minor": 150000,
            "currency": "INR",
            "ordered_at": "2026-01-01T10:00:00Z",
        })
        self.assertEqual(order.order_id, "ord_001")
        self.assertEqual(order.amount_minor, 150000)

    def test_rejects_non_positive_order_amount(self):
        with self.assertRaises(ValueError):
            OrderRecord.from_dict({
                "order_id": "ord_001",
                "amount_minor": 0,
                "currency": "INR",
                "ordered_at": "2026-01-01T10:00:00Z",
            })

    def test_payment_event_requires_order_id(self):
        with self.assertRaises(ValueError):
            PaymentEventRecord.from_dict({
                "event_id": "evt_001",
                "event_type": "payment_captured",
                "transaction_id": "txn_001",
                "order_id": None,
                "parent_payment_id": None,
                "amount_minor": 100000,
                "currency": "INR",
                "occurred_at": "2026-01-01T10:00:00Z",
                "received_at": "2026-01-01T10:01:00Z",
            })

    def test_payment_event_received_before_occurred_fails(self):
        with self.assertRaises(ValueError):
            PaymentEventRecord.from_dict({
                "event_id": "evt_001",
                "event_type": "payment_captured",
                "transaction_id": "txn_001",
                "order_id": "ord_001",
                "parent_payment_id": None,
                "amount_minor": 100000,
                "currency": "INR",
                "occurred_at": "2026-01-01T10:05:00Z",
                "received_at": "2026-01-01T10:00:00Z",
            })

    def test_refund_requires_parent_payment_id(self):
        with self.assertRaises(ValueError):
            PaymentEventRecord.from_dict({
                "event_id": "evt_002",
                "event_type": "refund_succeeded",
                "transaction_id": "rf_001",
                "order_id": None,
                "parent_payment_id": None,
                "amount_minor": 20000,
                "currency": "INR",
                "occurred_at": "2026-01-01T10:00:00Z",
                "received_at": "2026-01-01T10:01:00Z",
            })

    def test_settlement_entry_sign_validation(self):
        # Refund line with positive gross must be rejected
        with self.assertRaises(ValueError):
            SettlementEntryRecord.from_dict({
                "settlement_entry_id": "stl_001",
                "payout_id": "po_001",
                "transaction_id": "rf_001",
                "entry_type": "refund",
                "gross_minor": 50000,  # Should be negative
                "fee_minor": 0,
                "net_minor": 50000,
                "currency": "INR",
                "settled_at": "2026-01-02T04:00:00Z",
            })

    def test_manifest_rejects_path_traversal(self):
        with self.assertRaises(ValueError):
            BatchManifest.from_dict({
                "schema_version": "1.0.0",
                "batch_id": "batch_001",
                "scenario": "test",
                "seed": 1,
                "base_time": "2026-01-01T00:00:00Z",
                "delivered_at": "2026-01-01T01:00:00Z",
                "files": [
                    {
                        "filename": "../../../etc/passwd",
                        "source_name": "orders",
                        "record_count": 1,
                        "sha256_hex": "a" * 64,
                    }
                ],
            })


if __name__ == "__main__":
    unittest.main()
