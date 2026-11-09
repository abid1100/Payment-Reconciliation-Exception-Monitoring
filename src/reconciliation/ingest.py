"""Batch file ingestion, provenance recording, and deduplication via prepare_batch.

Consumes in-memory PreparedBatch; does not reopen unchecked input files.
Separates raw evidence staging from canonical business loading for transactional failure isolation.
Distinguishes staged raw evidence from successfully published inputs on retry/replay.
"""
from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from reconciliation.batch import BatchValidationError, PreparedBatch, prepare_batch
from reconciliation.contracts import (
    OrderRecord,
    PaymentEventRecord,
    SettlementEntryRecord,
    PayoutRecord,
)
from reconciliation.evidence import stage_batch_evidence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvaluatedDisposition:
    raw_record_id: int
    disposition: str
    rejection_reason: str | None
    canonical_id: str | None


@dataclass
class IngestionSummary:
    batch_id: str
    files_processed: int
    rows_seen: int
    rows_accepted: int
    rows_duplicate: int
    rows_conflict: int
    rows_rejected: int
    files_skipped: int = 0
    dispositions: list[EvaluatedDisposition] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "files_processed": self.files_processed,
            "files_skipped": self.files_skipped,
            "rows_seen": self.rows_seen,
            "rows_accepted": self.rows_accepted,
            "rows_duplicate": self.rows_duplicate,
            "rows_conflict": self.rows_conflict,
            "rows_rejected": self.rows_rejected,
        }


@dataclass
class AcceptedOrder:
    order_id: str
    amount_minor: int
    currency: str
    ordered_at: datetime.datetime
    raw_record_id: int


@dataclass
class AcceptedPaymentEvent:
    event_id: str
    event_type: str
    transaction_id: str
    order_id: str | None
    parent_payment_id: str | None
    amount_minor: int
    currency: str
    occurred_at: datetime.datetime
    received_at: datetime.datetime
    raw_record_id: int
    is_new_transaction: bool
    transaction_type: str


@dataclass
class AcceptedSettlementEntry:
    settlement_entry_id: str
    payout_id: str
    transaction_id: str
    entry_type: str
    gross_minor: int
    fee_minor: int
    net_minor: int
    currency: str
    settled_at: datetime.datetime
    raw_record_id: int


@dataclass
class AcceptedPayout:
    payout_id: str
    amount_minor: int
    expected_entry_count: int
    currency: str
    issued_at: datetime.datetime
    raw_record_id: int


def _insert_raw_record(
    cur: Any,
    batch_id: str,
    source_type: str,
    file_hash: str,
    line_number: int,
    raw_payload: str,
    parse_status: str,
    rejection_reason: str | None = None,
    canonical_id: str | None = None,
    returning_id: bool = False,
) -> int | None:
    query = """
        INSERT INTO raw.raw_records (
            batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason, canonical_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    if returning_id:
        query += " RETURNING record_id;"
    else:
        query += ";"

    cur.execute(
        query,
        (batch_id, source_type, file_hash, line_number, raw_payload, parse_status, rejection_reason, canonical_id),
    )
    if returning_id:
        row = cur.fetchone()
        if row is not None:
            return row["record_id"] if isinstance(row, dict) else row[0]
        return None
    return None


def is_file_published(conn: Any, source_type: str, file_hash: str) -> bool:
    """Check if an input file (source_type, file_hash) was already successfully published in a SUCCEEDED run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_summary
            FROM raw.pipeline_runs
            WHERE status = 'SUCCEEDED';
            """
        )
        rows = cur.fetchall()
        for row in rows:
            summary = row["input_summary"] if isinstance(row, dict) else row[0]
            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    continue
            if isinstance(summary, dict):
                markers = summary.get("markers", {})
                pub_files = markers.get("published_files", [])
                for pf in pub_files:
                    if pf.get("source_type") == source_type and pf.get("file_hash") == file_hash:
                        return True

        return False


def validate_batch_files(batch_dir: str | Path, as_of: str | datetime.datetime | None = None) -> list[str]:
    """Validate batch files and record contracts via prepare_batch without database writes."""
    try:
        prepared = prepare_batch(batch_dir, as_of=as_of)
        errors = [
            f"{file.filename} line {row.line_number}: {error}"
            for file in prepared.files
            for row in file.rows
            for error in row.errors
        ]
        return errors
    except (BatchValidationError, ValueError) as e:
        return [str(e)]


def stage_raw_batch_evidence(
    conn: Any,
    batch: PreparedBatch | str | Path,
    as_of: str | datetime.datetime | None = None,
) -> tuple[IngestionSummary, list[Any]]:
    """Stage raw evidence in raw.source_batches and raw.raw_records.

    Persists exact raw input lines and provenance. Reuses preserved evidence without
    duplicating physical records on retries. Re-evaluates row dispositions against
    current canonical state, returning accepted canonical records ready for transactional
    loading into core.*.
    """
    if not isinstance(batch, PreparedBatch):
        batch = prepare_batch(batch, as_of=as_of)

    manifest = batch.manifest

    # Step 1: Explicit evidence transaction before row staging, file skipping, or all-invalid rejection.
    # Persists source batch, manifest/file blobs and associations atomically.
    if hasattr(conn, "transaction"):
        with conn.transaction(savepoint_name="evidence"):
            stage_batch_evidence(conn, batch)
    else:
        stage_batch_evidence(conn, batch)
        if hasattr(conn, "commit"):
            conn.commit()

    summary = IngestionSummary(
        batch_id=manifest.batch_id,
        files_processed=0,
        rows_seen=0,
        rows_accepted=0,
        rows_duplicate=0,
        rows_conflict=0,
        rows_rejected=0,
        files_skipped=0,
    )

    accepted_records: list[Any] = []
    evaluated_dispositions: list[EvaluatedDisposition] = []

    # In-flight intra-batch registries to detect duplicates/conflicts within the same batch
    seen_orders: dict[str, dict[str, Any]] = {}
    seen_events: dict[str, dict[str, Any]] = {}
    seen_txns: dict[str, dict[str, Any]] = {}
    seen_settlements: dict[str, dict[str, Any]] = {}
    seen_payouts: dict[str, dict[str, Any]] = {}

    with conn.cursor() as cur:
        for prep_file in batch.files:
            source_name = prep_file.source_name
            file_hash = prep_file.file_hash

            # Skip file only if it was already successfully published in a prior SUCCEEDED run
            if is_file_published(conn, source_name, file_hash):
                logger.info("File '%s' (%s, hash=%s) already published; skipping.", prep_file.filename, source_name, file_hash)
                summary.files_skipped += 1
                continue

            summary.files_processed += 1

            for row in prep_file.rows:
                summary.rows_seen += 1
                line_number = row.line_number
                # Display payload safe for storage when raw input contains NUL characters
                raw_payload = row.raw_text.replace("\x00", "\\0")

                # Check if this physical row was already staged in raw.raw_records
                cur.execute(
                    """
                    SELECT record_id, parse_status, rejection_reason, canonical_id
                    FROM raw.raw_records
                    WHERE source_type = %s AND file_hash = %s AND line_number = %s
                    LIMIT 1;
                    """,
                    (source_name, file_hash, line_number),
                )
                existing_raw = cur.fetchone()
                existing_rec_id = None
                if existing_raw is not None:
                    existing_rec_id = existing_raw["record_id"] if isinstance(existing_raw, dict) else existing_raw[0]

                # 1. Row rejected during preparation/validation
                if row.record is None or row.errors:
                    disposition = "REJECTED"
                    reason = "; ".join(row.errors) if row.errors else "Malformed record"
                    canonical_id = None
                    summary.rows_rejected += 1

                else:
                    rec = row.record

                    # 2. Orders Ingestion
                    if isinstance(rec, OrderRecord):
                        existing = seen_orders.get(rec.order_id)
                        if existing is None:
                            cur.execute(
                                "SELECT amount_minor, currency, ordered_at FROM core.orders WHERE order_id = %s;",
                                (rec.order_id,),
                            )
                            db_row = cur.fetchone()
                            if db_row is not None:
                                existing = {
                                    "amount_minor": db_row["amount_minor"] if isinstance(db_row, dict) else db_row[0],
                                    "currency": db_row["currency"] if isinstance(db_row, dict) else db_row[1],
                                    "ordered_at": db_row["ordered_at"] if isinstance(db_row, dict) else db_row[2],
                                }

                        if existing is None:
                            disposition = "ACCEPTED"
                            reason = None
                            canonical_id = rec.order_id
                            seen_orders[rec.order_id] = {
                                "amount_minor": rec.amount_minor,
                                "currency": rec.currency,
                                "ordered_at": rec.ordered_at,
                            }
                            summary.rows_accepted += 1
                        else:
                            if (
                                existing["amount_minor"] == rec.amount_minor
                                and existing["currency"] == rec.currency
                                and existing["ordered_at"] == rec.ordered_at
                            ):
                                disposition = "DUPLICATE"
                                reason = None
                                canonical_id = rec.order_id
                                summary.rows_duplicate += 1
                            else:
                                disposition = "CONFLICT"
                                reason = "Conflicting business payload for existing order_id"
                                canonical_id = rec.order_id
                                summary.rows_conflict += 1

                    # 3. Payment Events Ingestion
                    elif isinstance(rec, PaymentEventRecord):
                        existing_ev = seen_events.get(rec.event_id)
                        if existing_ev is None:
                            cur.execute(
                                "SELECT amount_minor, currency, occurred_at, event_type, transaction_id, order_id, parent_payment_id FROM core.payment_events WHERE event_id = %s;",
                                (rec.event_id,),
                            )
                            db_row = cur.fetchone()
                            if db_row is not None:
                                existing_ev = {
                                    "amount_minor": db_row["amount_minor"] if isinstance(db_row, dict) else db_row[0],
                                    "currency": db_row["currency"] if isinstance(db_row, dict) else db_row[1],
                                    "occurred_at": db_row["occurred_at"] if isinstance(db_row, dict) else db_row[2],
                                    "event_type": db_row["event_type"] if isinstance(db_row, dict) else db_row[3],
                                    "transaction_id": db_row["transaction_id"] if isinstance(db_row, dict) else db_row[4],
                                    "order_id": db_row["order_id"] if isinstance(db_row, dict) else db_row[5],
                                    "parent_payment_id": db_row["parent_payment_id"] if isinstance(db_row, dict) else db_row[6],
                                }

                        if existing_ev is not None:
                            if (
                                existing_ev["amount_minor"] == rec.amount_minor
                                and existing_ev["currency"] == rec.currency
                                and existing_ev["occurred_at"] == rec.occurred_at
                                and existing_ev["event_type"] == rec.event_type
                                and existing_ev["transaction_id"] == rec.transaction_id
                                and existing_ev["order_id"] == rec.order_id
                                and existing_ev["parent_payment_id"] == rec.parent_payment_id
                            ):
                                disposition = "DUPLICATE"
                                reason = None
                                canonical_id = rec.event_id
                                summary.rows_duplicate += 1
                            else:
                                disposition = "CONFLICT"
                                reason = "Conflicting business payload for existing event_id"
                                canonical_id = rec.event_id
                                summary.rows_conflict += 1
                        else:
                            expected_txn_type = "payment" if rec.event_type == "payment_captured" else "refund"
                            existing_txn = seen_txns.get(rec.transaction_id)
                            if existing_txn is None:
                                cur.execute(
                                    "SELECT transaction_type, order_id, parent_payment_id, amount_minor, currency, occurred_at FROM core.transactions WHERE transaction_id = %s;",
                                    (rec.transaction_id,),
                                )
                                db_txn = cur.fetchone()
                                if db_txn is not None:
                                    existing_txn = {
                                        "transaction_type": db_txn["transaction_type"] if isinstance(db_txn, dict) else db_txn[0],
                                        "order_id": db_txn["order_id"] if isinstance(db_txn, dict) else db_txn[1],
                                        "parent_payment_id": db_txn["parent_payment_id"] if isinstance(db_txn, dict) else db_txn[2],
                                        "amount_minor": db_txn["amount_minor"] if isinstance(db_txn, dict) else db_txn[3],
                                        "currency": db_txn["currency"] if isinstance(db_txn, dict) else db_txn[4],
                                        "occurred_at": db_txn["occurred_at"] if isinstance(db_txn, dict) else db_txn[5],
                                    }

                            if existing_txn is not None:
                                compatible = (
                                    existing_txn["transaction_type"] == expected_txn_type
                                    and existing_txn["order_id"] == rec.order_id
                                    and existing_txn["parent_payment_id"] == rec.parent_payment_id
                                    and existing_txn["amount_minor"] == rec.amount_minor
                                    and existing_txn["currency"] == rec.currency
                                    and existing_txn["occurred_at"] == rec.occurred_at
                                )
                                if not compatible:
                                    disposition = "CONFLICT"
                                    reason = "Conflicting business payload for existing transaction_id"
                                    canonical_id = rec.event_id
                                    summary.rows_conflict += 1
                                else:
                                    disposition = "ACCEPTED"
                                    reason = None
                                    canonical_id = rec.event_id
                                    is_new_txn = False
                                    seen_events[rec.event_id] = {
                                        "amount_minor": rec.amount_minor,
                                        "currency": rec.currency,
                                        "occurred_at": rec.occurred_at,
                                        "event_type": rec.event_type,
                                        "transaction_id": rec.transaction_id,
                                        "order_id": rec.order_id,
                                        "parent_payment_id": rec.parent_payment_id,
                                    }
                                    summary.rows_accepted += 1
                            else:
                                disposition = "ACCEPTED"
                                reason = None
                                canonical_id = rec.event_id
                                is_new_txn = True
                                seen_events[rec.event_id] = {
                                    "amount_minor": rec.amount_minor,
                                    "currency": rec.currency,
                                    "occurred_at": rec.occurred_at,
                                    "event_type": rec.event_type,
                                    "transaction_id": rec.transaction_id,
                                    "order_id": rec.order_id,
                                    "parent_payment_id": rec.parent_payment_id,
                                }
                                seen_txns[rec.transaction_id] = {
                                    "transaction_type": expected_txn_type,
                                    "order_id": rec.order_id,
                                    "parent_payment_id": rec.parent_payment_id,
                                    "amount_minor": rec.amount_minor,
                                    "currency": rec.currency,
                                    "occurred_at": rec.occurred_at,
                                }
                                summary.rows_accepted += 1

                    # 4. Settlement Entries Ingestion
                    elif isinstance(rec, SettlementEntryRecord):
                        existing = seen_settlements.get(rec.settlement_entry_id)
                        if existing is None:
                            cur.execute(
                                "SELECT gross_minor, fee_minor, net_minor, currency, settled_at, payout_id, transaction_id, entry_type FROM core.settlement_entries WHERE settlement_entry_id = %s;",
                                (rec.settlement_entry_id,),
                            )
                            db_row = cur.fetchone()
                            if db_row is not None:
                                existing = {
                                    "gross_minor": db_row["gross_minor"] if isinstance(db_row, dict) else db_row[0],
                                    "fee_minor": db_row["fee_minor"] if isinstance(db_row, dict) else db_row[1],
                                    "net_minor": db_row["net_minor"] if isinstance(db_row, dict) else db_row[2],
                                    "currency": db_row["currency"] if isinstance(db_row, dict) else db_row[3],
                                    "settled_at": db_row["settled_at"] if isinstance(db_row, dict) else db_row[4],
                                    "payout_id": db_row["payout_id"] if isinstance(db_row, dict) else db_row[5],
                                    "transaction_id": db_row["transaction_id"] if isinstance(db_row, dict) else db_row[6],
                                    "entry_type": db_row["entry_type"] if isinstance(db_row, dict) else db_row[7],
                                }

                        if existing is None:
                            disposition = "ACCEPTED"
                            reason = None
                            canonical_id = rec.settlement_entry_id
                            seen_settlements[rec.settlement_entry_id] = {
                                "gross_minor": rec.gross_minor,
                                "fee_minor": rec.fee_minor,
                                "net_minor": rec.net_minor,
                                "currency": rec.currency,
                                "settled_at": rec.settled_at,
                                "payout_id": rec.payout_id,
                                "transaction_id": rec.transaction_id,
                                "entry_type": rec.entry_type,
                            }
                            summary.rows_accepted += 1
                        else:
                            if (
                                existing["gross_minor"] == rec.gross_minor
                                and existing["fee_minor"] == rec.fee_minor
                                and existing["net_minor"] == rec.net_minor
                                and existing["currency"] == rec.currency
                                and existing["settled_at"] == rec.settled_at
                                and existing["payout_id"] == rec.payout_id
                                and existing["transaction_id"] == rec.transaction_id
                                and existing["entry_type"] == rec.entry_type
                            ):
                                disposition = "DUPLICATE"
                                reason = None
                                canonical_id = rec.settlement_entry_id
                                summary.rows_duplicate += 1
                            else:
                                disposition = "CONFLICT"
                                reason = "Conflicting business payload for existing settlement_entry_id"
                                canonical_id = rec.settlement_entry_id
                                summary.rows_conflict += 1

                    # 5. Payouts Ingestion
                    elif isinstance(rec, PayoutRecord):
                        existing = seen_payouts.get(rec.payout_id)
                        if existing is None:
                            cur.execute(
                                "SELECT amount_minor, expected_entry_count, currency, issued_at FROM core.payouts WHERE payout_id = %s;",
                                (rec.payout_id,),
                            )
                            db_row = cur.fetchone()
                            if db_row is not None:
                                existing = {
                                    "amount_minor": db_row["amount_minor"] if isinstance(db_row, dict) else db_row[0],
                                    "expected_entry_count": db_row["expected_entry_count"] if isinstance(db_row, dict) else db_row[1],
                                    "currency": db_row["currency"] if isinstance(db_row, dict) else db_row[2],
                                    "issued_at": db_row["issued_at"] if isinstance(db_row, dict) else db_row[3],
                                }

                        if existing is None:
                            disposition = "ACCEPTED"
                            reason = None
                            canonical_id = rec.payout_id
                            seen_payouts[rec.payout_id] = {
                                "amount_minor": rec.amount_minor,
                                "expected_entry_count": rec.expected_entry_count,
                                "currency": rec.currency,
                                "issued_at": rec.issued_at,
                            }
                            summary.rows_accepted += 1
                        else:
                            if (
                                existing["amount_minor"] == rec.amount_minor
                                and existing["expected_entry_count"] == rec.expected_entry_count
                                and existing["currency"] == rec.currency
                                and existing["issued_at"] == rec.issued_at
                            ):
                                disposition = "DUPLICATE"
                                reason = None
                                canonical_id = rec.payout_id
                                summary.rows_duplicate += 1
                            else:
                                disposition = "CONFLICT"
                                reason = "Conflicting business payload for existing payout_id"
                                canonical_id = rec.payout_id
                                summary.rows_conflict += 1

                # Persist raw record evidence without duplicating physical records (strictly append-only)
                if existing_rec_id is None:
                    raw_rec_id = _insert_raw_record(
                        cur,
                        manifest.batch_id,
                        source_name,
                        file_hash,
                        line_number,
                        raw_payload,
                        disposition,
                        rejection_reason=reason,
                        canonical_id=canonical_id,
                        returning_id=True,
                    )
                else:
                    raw_rec_id = existing_rec_id

                evaluated_dispositions.append(
                    EvaluatedDisposition(
                        raw_record_id=raw_rec_id,
                        disposition=disposition,
                        rejection_reason=reason,
                        canonical_id=canonical_id,
                    )
                )

                # Collect accepted records for transactional canonical loading
                if disposition == "ACCEPTED":
                    if isinstance(row.record, OrderRecord):
                        accepted_records.append(
                            AcceptedOrder(
                                order_id=row.record.order_id,
                                amount_minor=row.record.amount_minor,
                                currency=row.record.currency,
                                ordered_at=row.record.ordered_at,
                                raw_record_id=raw_rec_id,  # type: ignore
                            )
                        )
                    elif isinstance(row.record, PaymentEventRecord):
                        accepted_records.append(
                            AcceptedPaymentEvent(
                                event_id=row.record.event_id,
                                event_type=row.record.event_type,
                                transaction_id=row.record.transaction_id,
                                order_id=row.record.order_id,
                                parent_payment_id=row.record.parent_payment_id,
                                amount_minor=row.record.amount_minor,
                                currency=row.record.currency,
                                occurred_at=row.record.occurred_at,
                                received_at=row.record.received_at,
                                raw_record_id=raw_rec_id,  # type: ignore
                                is_new_transaction=is_new_txn,
                                transaction_type=expected_txn_type,
                            )
                        )
                    elif isinstance(row.record, SettlementEntryRecord):
                        accepted_records.append(
                            AcceptedSettlementEntry(
                                settlement_entry_id=row.record.settlement_entry_id,
                                payout_id=row.record.payout_id,
                                transaction_id=row.record.transaction_id,
                                entry_type=row.record.entry_type,
                                gross_minor=row.record.gross_minor,
                                fee_minor=row.record.fee_minor,
                                net_minor=row.record.net_minor,
                                currency=row.record.currency,
                                settled_at=row.record.settled_at,
                                raw_record_id=raw_rec_id,  # type: ignore
                            )
                        )
                    elif isinstance(row.record, PayoutRecord):
                        accepted_records.append(
                            AcceptedPayout(
                                payout_id=row.record.payout_id,
                                amount_minor=row.record.amount_minor,
                                expected_entry_count=row.record.expected_entry_count,
                                currency=row.record.currency,
                                issued_at=row.record.issued_at,
                                raw_record_id=raw_rec_id,  # type: ignore
                            )
                        )

        # Check all-invalid batch rule evaluated against the whole delivered input batch.
        # Staging of physical evidence is complete above, preserving rejected rows.
        total_batch_rows = sum(len(f.rows) for f in batch.files)
        total_batch_valid = sum(1 for f in batch.files for r in f.rows if r.record is not None and not r.errors)
        if total_batch_rows > 0 and total_batch_valid == 0:
            raise ValueError(f"All input rows failed validation in batch '{manifest.batch_id}'; batch rejected.")

    summary.dispositions = evaluated_dispositions
    return summary, accepted_records


def load_canonical_facts(conn: Any, accepted_records: list[Any], run_sequence: int) -> None:
    """Load pre-validated accepted records into canonical core.* tables under the business transaction."""
    with conn.cursor() as cur:
        for item in accepted_records:
            if isinstance(item, AcceptedOrder):
                cur.execute(
                    """
                    INSERT INTO core.orders (order_id, amount_minor, currency, ordered_at, loaded_run_sequence, raw_record_id)
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (item.order_id, item.amount_minor, item.currency, item.ordered_at, run_sequence, item.raw_record_id),
                )
            elif isinstance(item, AcceptedPaymentEvent):
                cur.execute(
                    """
                    INSERT INTO core.payment_events (
                        event_id, event_type, transaction_id, order_id, parent_payment_id,
                        amount_minor, currency, occurred_at, received_at, loaded_run_sequence, raw_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        item.event_id,
                        item.event_type,
                        item.transaction_id,
                        item.order_id,
                        item.parent_payment_id,
                        item.amount_minor,
                        item.currency,
                        item.occurred_at,
                        item.received_at,
                        run_sequence,
                        item.raw_record_id,
                    ),
                )
                if item.is_new_transaction:
                    cur.execute(
                        """
                        INSERT INTO core.transactions (
                            transaction_id, transaction_type, order_id, parent_payment_id,
                            amount_minor, currency, occurred_at, first_event_id, loaded_run_sequence
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            item.transaction_id,
                            item.transaction_type,
                            item.order_id,
                            item.parent_payment_id,
                            item.amount_minor,
                            item.currency,
                            item.occurred_at,
                            item.event_id,
                            run_sequence,
                        ),
                    )
            elif isinstance(item, AcceptedSettlementEntry):
                cur.execute(
                    """
                    INSERT INTO core.settlement_entries (
                        settlement_entry_id, payout_id, transaction_id, entry_type,
                        gross_minor, fee_minor, net_minor, currency, settled_at,
                        loaded_run_sequence, raw_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        item.settlement_entry_id,
                        item.payout_id,
                        item.transaction_id,
                        item.entry_type,
                        item.gross_minor,
                        item.fee_minor,
                        item.net_minor,
                        item.currency,
                        item.settled_at,
                        run_sequence,
                        item.raw_record_id,
                    ),
                )
            elif isinstance(item, AcceptedPayout):
                cur.execute(
                    """
                    INSERT INTO core.payouts (
                        payout_id, amount_minor, expected_entry_count, currency,
                        issued_at, loaded_run_sequence, raw_record_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        item.payout_id,
                        item.amount_minor,
                        item.expected_entry_count,
                        item.currency,
                        item.issued_at,
                        run_sequence,
                        item.raw_record_id,
                    ),
                )


def ingest_batch_into_database(
    conn: Any,
    batch: PreparedBatch | str | Path,
    run_sequence: int,
    as_of: str | datetime.datetime | None = None,
) -> IngestionSummary:
    """Ingest prepared batch records into raw and core tables with strict provenance and deduplication.

    Stages raw evidence and then loads canonical facts.
    """
    summary, accepted = stage_raw_batch_evidence(conn=conn, batch=batch, as_of=as_of)
    load_canonical_facts(conn=conn, accepted_records=accepted, run_sequence=run_sequence)
    return summary
