"""Batch file ingestion, cryptographic verification, provenance recording, and deduplication."""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from reconciliation.contracts import (
    BatchManifest,
    OrderRecord,
    PaymentEventRecord,
    SettlementEntryRecord,
    PayoutRecord,
)
from reconciliation.generate import calculate_sha256

logger = logging.getLogger(__name__)


@dataclass
class IngestionSummary:
    batch_id: str
    files_processed: int
    rows_seen: int
    rows_accepted: int
    rows_duplicate: int
    rows_conflict: int
    rows_rejected: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "files_processed": self.files_processed,
            "rows_seen": self.rows_seen,
            "rows_accepted": self.rows_accepted,
            "rows_duplicate": self.rows_duplicate,
            "rows_conflict": self.rows_conflict,
            "rows_rejected": self.rows_rejected,
        }


def validate_batch_files(batch_dir: str | Path) -> list[str]:
    """Validate manifest file hashes and record contracts without database writes."""
    path = Path(batch_dir)
    manifest_file = path / "manifest.json"
    if not manifest_file.is_file():
        return [f"Missing manifest.json in {path}"]

    errors: list[str] = []
    try:
        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)
        manifest = BatchManifest.from_dict(manifest_data)
    except Exception as e:
        return [f"Manifest error: {e}"]

    for entry in manifest.files:
        fpath = path / entry.filename
        if not fpath.is_file():
            errors.append(f"Missing referenced file: {entry.filename}")
            continue

        actual_hash = calculate_sha256(fpath)
        if actual_hash != entry.sha256_hex.lower():
            errors.append(
                f"Hash mismatch on {entry.filename}: expected {entry.sha256_hex}, got {actual_hash}"
            )
            continue

        # Validate rows
        line_num = 0
        try:
            if entry.source_name == "orders":
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        line_num += 1
                        OrderRecord.from_dict(row)
            elif entry.source_name == "payment_events":
                with open(fpath, "r", encoding="utf-8") as f:
                    for line in f:
                        line_num += 1
                        if line.strip():
                            data = json.loads(line)
                            PaymentEventRecord.from_dict(data)
            elif entry.source_name == "settlement_entries":
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        line_num += 1
                        SettlementEntryRecord.from_dict(row)
            elif entry.source_name == "payouts":
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        line_num += 1
                        PayoutRecord.from_dict(row)
        except Exception as e:
            errors.append(f"Error in {entry.filename} line {line_num}: {e}")

    return errors


def ingest_batch_into_database(
    conn: Any,
    batch_dir: str | Path,
    run_sequence: int,
) -> IngestionSummary:
    """Ingest batch records into raw and core tables with strict provenance and deduplication."""
    path = Path(batch_dir)
    manifest_file = path / "manifest.json"
    with open(manifest_file, "r", encoding="utf-8") as f:
        manifest = BatchManifest.from_dict(json.load(f))

    manifest_hash = calculate_sha256(manifest_file)

    summary = IngestionSummary(
        batch_id=manifest.batch_id,
        files_processed=len(manifest.files),
        rows_seen=0,
        rows_accepted=0,
        rows_duplicate=0,
        rows_conflict=0,
        rows_rejected=0,
    )

    with conn.cursor() as cur:
        # Register or verify source_batch
        cur.execute(
            """
            INSERT INTO raw.source_batches (batch_id, manifest_hash, schema_version, delivered_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (batch_id) DO UPDATE SET staged_at = CLOCK_TIMESTAMP();
            """,
            (manifest.batch_id, manifest_hash, manifest.schema_version, manifest.delivered_at),
        )

        for entry in manifest.files:
            fpath = path / entry.filename
            file_hash = calculate_sha256(fpath)

            if entry.source_name == "orders":
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for line_idx, row in enumerate(reader, start=2):
                        summary.rows_seen += 1
                        raw_str = json.dumps(row)
                        try:
                            order = OrderRecord.from_dict(row)
                            # Check deduplication vs conflict
                            cur.execute("SELECT amount_minor, currency FROM core.orders WHERE order_id = %s;", (order.order_id,))
                            existing = cur.fetchone()
                            if existing is None:
                                cur.execute(
                                    """
                                    INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                    VALUES (%s, 'orders', %s, %s, %s, 'ACCEPTED', %s)
                                    RETURNING record_id;
                                    """,
                                    (manifest.batch_id, file_hash, line_idx, raw_str, order.order_id),
                                )
                                raw_rec_id = cur.fetchone()["record_id"]
                                cur.execute(
                                    """
                                    INSERT INTO core.orders (order_id, amount_minor, currency, ordered_at, loaded_run_sequence, raw_record_id)
                                    VALUES (%s, %s, %s, %s, %s, %s);
                                    """,
                                    (order.order_id, order.amount_minor, order.currency, order.ordered_at, run_sequence, raw_rec_id),
                                )
                                summary.rows_accepted += 1
                            else:
                                if existing["amount_minor"] == order.amount_minor and existing["currency"] == order.currency:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                        VALUES (%s, 'orders', %s, %s, %s, 'DUPLICATE', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, order.order_id),
                                    )
                                    summary.rows_duplicate += 1
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason, canonical_id)
                                        VALUES (%s, 'orders', %s, %s, %s, 'CONFLICT', 'Conflicting business payload for existing order_id', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, order.order_id),
                                    )
                                    summary.rows_conflict += 1
                        except Exception as e:
                            cur.execute(
                                """
                                INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason)
                                VALUES (%s, 'orders', %s, %s, %s, 'REJECTED', %s);
                                """,
                                (manifest.batch_id, file_hash, line_idx, raw_str, str(e)),
                            )
                            summary.rows_rejected += 1

            elif entry.source_name == "payment_events":
                with open(fpath, "r", encoding="utf-8") as f:
                    for line_idx, line in enumerate(f, start=1):
                        if not line.strip():
                            continue
                        summary.rows_seen += 1
                        raw_str = line.strip()
                        try:
                            data = json.loads(raw_str)
                            event = PaymentEventRecord.from_dict(data)
                            cur.execute("SELECT amount_minor, currency FROM core.payment_events WHERE event_id = %s;", (event.event_id,))
                            existing = cur.fetchone()
                            if existing is None:
                                cur.execute(
                                    """
                                    INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                    VALUES (%s, 'payment_events', %s, %s, %s, 'ACCEPTED', %s)
                                    RETURNING record_id;
                                    """,
                                    (manifest.batch_id, file_hash, line_idx, raw_str, event.event_id),
                                )
                                raw_rec_id = cur.fetchone()["record_id"]
                                cur.execute(
                                    """
                                    INSERT INTO core.payment_events (event_id, event_type, transaction_id, order_id, parent_payment_id, amount_minor, currency, occurred_at, received_at, loaded_run_sequence, raw_record_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    """,
                                    (
                                        event.event_id, event.event_type, event.transaction_id, event.order_id,
                                        event.parent_payment_id, event.amount_minor, event.currency,
                                        event.occurred_at, event.received_at, run_sequence, raw_rec_id
                                    ),
                                )
                                # Maintain deduplicated core.transactions
                                txn_type = "payment" if event.event_type == "payment_captured" else "refund"
                                cur.execute("SELECT transaction_id FROM core.transactions WHERE transaction_id = %s;", (event.transaction_id,))
                                if cur.fetchone() is None:
                                    cur.execute(
                                        """
                                        INSERT INTO core.transactions (transaction_id, transaction_type, order_id, parent_payment_id, amount_minor, currency, occurred_at, first_event_id, loaded_run_sequence)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                                        """,
                                        (
                                            event.transaction_id, txn_type, event.order_id, event.parent_payment_id,
                                            event.amount_minor, event.currency, event.occurred_at, event.event_id, run_sequence
                                        ),
                                    )
                                summary.rows_accepted += 1
                            else:
                                if existing["amount_minor"] == event.amount_minor and existing["currency"] == event.currency:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                        VALUES (%s, 'payment_events', %s, %s, %s, 'DUPLICATE', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, event.event_id),
                                    )
                                    summary.rows_duplicate += 1
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason, canonical_id)
                                        VALUES (%s, 'payment_events', %s, %s, %s, 'CONFLICT', 'Conflicting business payload for existing event_id', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, event.event_id),
                                    )
                                    summary.rows_conflict += 1
                        except Exception as e:
                            cur.execute(
                                """
                                INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason)
                                VALUES (%s, 'payment_events', %s, %s, %s, 'REJECTED', %s);
                                """,
                                (manifest.batch_id, file_hash, line_idx, raw_str, str(e)),
                            )
                            summary.rows_rejected += 1

            elif entry.source_name == "settlement_entries":
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for line_idx, row in enumerate(reader, start=2):
                        summary.rows_seen += 1
                        raw_str = json.dumps(row)
                        try:
                            stl = SettlementEntryRecord.from_dict(row)
                            cur.execute("SELECT gross_minor, net_minor FROM core.settlement_entries WHERE settlement_entry_id = %s;", (stl.settlement_entry_id,))
                            existing = cur.fetchone()
                            if existing is None:
                                cur.execute(
                                    """
                                    INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                    VALUES (%s, 'settlement_entries', %s, %s, %s, 'ACCEPTED', %s)
                                    RETURNING record_id;
                                    """,
                                    (manifest.batch_id, file_hash, line_idx, raw_str, stl.settlement_entry_id),
                                )
                                raw_rec_id = cur.fetchone()["record_id"]
                                cur.execute(
                                    """
                                    INSERT INTO core.settlement_entries (settlement_entry_id, payout_id, transaction_id, entry_type, gross_minor, fee_minor, net_minor, currency, settled_at, loaded_run_sequence, raw_record_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                    """,
                                    (
                                        stl.settlement_entry_id, stl.payout_id, stl.transaction_id, stl.entry_type,
                                        stl.gross_minor, stl.fee_minor, stl.net_minor, stl.currency, stl.settled_at,
                                        run_sequence, raw_rec_id
                                    ),
                                )
                                summary.rows_accepted += 1
                            else:
                                if existing["gross_minor"] == stl.gross_minor and existing["net_minor"] == stl.net_minor:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                        VALUES (%s, 'settlement_entries', %s, %s, %s, 'DUPLICATE', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, stl.settlement_entry_id),
                                    )
                                    summary.rows_duplicate += 1
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason, canonical_id)
                                        VALUES (%s, 'settlement_entries', %s, %s, %s, 'CONFLICT', 'Conflicting business payload for existing settlement_entry_id', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, stl.settlement_entry_id),
                                    )
                                    summary.rows_conflict += 1
                        except Exception as e:
                            cur.execute(
                                """
                                INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason)
                                VALUES (%s, 'settlement_entries', %s, %s, %s, 'REJECTED', %s);
                                """,
                                (manifest.batch_id, file_hash, line_idx, raw_str, str(e)),
                            )
                            summary.rows_rejected += 1

            elif entry.source_name == "payouts":
                with open(fpath, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for line_idx, row in enumerate(reader, start=2):
                        summary.rows_seen += 1
                        raw_str = json.dumps(row)
                        try:
                            po = PayoutRecord.from_dict(row)
                            cur.execute("SELECT amount_minor FROM core.payouts WHERE payout_id = %s;", (po.payout_id,))
                            existing = cur.fetchone()
                            if existing is None:
                                cur.execute(
                                    """
                                    INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                    VALUES (%s, 'payouts', %s, %s, %s, 'ACCEPTED', %s)
                                    RETURNING record_id;
                                    """,
                                    (manifest.batch_id, file_hash, line_idx, raw_str, po.payout_id),
                                )
                                raw_rec_id = cur.fetchone()["record_id"]
                                cur.execute(
                                    """
                                    INSERT INTO core.payouts (payout_id, amount_minor, expected_entry_count, currency, issued_at, loaded_run_sequence, raw_record_id)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                                    """,
                                    (po.payout_id, po.amount_minor, po.expected_entry_count, po.currency, po.issued_at, run_sequence, raw_rec_id),
                                )
                                summary.rows_accepted += 1
                            else:
                                if existing["amount_minor"] == po.amount_minor:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, canonical_id)
                                        VALUES (%s, 'payouts', %s, %s, %s, 'DUPLICATE', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, po.payout_id),
                                    )
                                    summary.rows_duplicate += 1
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason, canonical_id)
                                        VALUES (%s, 'payouts', %s, %s, %s, 'CONFLICT', 'Conflicting business payload for existing payout_id', %s);
                                        """,
                                        (manifest.batch_id, file_hash, line_idx, raw_str, po.payout_id),
                                    )
                                    summary.rows_conflict += 1
                        except Exception as e:
                            cur.execute(
                                """
                                INSERT INTO raw.raw_records (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason)
                                VALUES (%s, 'payouts', %s, %s, %s, 'REJECTED', %s);
                                """,
                                (manifest.batch_id, file_hash, line_idx, raw_str, str(e)),
                            )
                            summary.rows_rejected += 1

    return summary
