"""Deterministic synthetic batch generator for reconciliation scenarios."""
from __future__ import annotations

import csv
import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from reconciliation.contracts import parse_utc_timestamp, validate_identifier


def calculate_sha256(filepath: Path) -> str:
    """Compute SHA-256 hex digest of file bytes."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest().lower()


def generate_batch(
    output_dir: str | Path,
    scenario: str = "baseline",
    seed: int = 42,
    base_time: str = "2026-01-01T00:00:00Z",
    batch_id: str | None = None,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Generate the fixed hand-checkable v1 fixture (both scenario aliases).

    Seed labels provenance only: it does not randomize the fixture. Default
    identity is batch_{scenario}_{seed}; changing base_time under that identity
    is a different payload, so use an explicit new batch_id for a new batch.
    CSV uses CRLF; JSONL and manifest use LF on every platform. Overwrite updates
    only these five generated files, never removes unrelated output files.
    """
    if scenario not in ("baseline", "sample_baseline_with_edge_cases"):
        raise ValueError("Unsupported generator scenario")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")
    if not isinstance(base_time, str):
        raise ValueError("base_time must be an ISO-8601 timestamp string")
    base_dt = parse_utc_timestamp(base_time, "base_time")
    try:
        delivered_at = base_dt + datetime.timedelta(days=2, hours=10)
    except OverflowError:
        raise ValueError("base_time is too late for the fixture timestamps") from None
    if batch_id is None:
        batch_id = f"batch_{scenario}_{seed}"
    validate_identifier(batch_id, "batch_id")
    out = Path(output_dir)
    if out.exists() and any(out.iterdir()) and not overwrite:
        raise ValueError("Output directory is nonempty; pass overwrite=True or --overwrite explicitly")
    out.mkdir(parents=True, exist_ok=True)

    orders: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    settlements: list[dict[str, Any]] = []
    payouts: list[dict[str, Any]] = []

    if scenario in ("baseline", "sample_baseline_with_edge_cases"):
        # 1. Clean order & payment & settlement
        orders.append({
            "order_id": "ord_101",
            "amount_minor": 500000,
            "currency": "INR",
            "ordered_at": (base_dt + datetime.timedelta(hours=10)).isoformat(),
        })
        events.append({
            "event_id": "evt_201",
            "event_type": "payment_captured",
            "transaction_id": "txn_301",
            "order_id": "ord_101",
            "parent_payment_id": None,
            "amount_minor": 500000,
            "currency": "INR",
            "occurred_at": (base_dt + datetime.timedelta(hours=10, minutes=1)).isoformat(),
            "received_at": (base_dt + datetime.timedelta(hours=10, minutes=1, seconds=5)).isoformat(),
        })
        # Duplicate delivery of evt_201
        events.append({
            "event_id": "evt_201",
            "event_type": "payment_captured",
            "transaction_id": "txn_301",
            "order_id": "ord_101",
            "parent_payment_id": None,
            "amount_minor": 500000,
            "currency": "INR",
            "occurred_at": (base_dt + datetime.timedelta(hours=10, minutes=1)).isoformat(),
            "received_at": (base_dt + datetime.timedelta(hours=10, minutes=5)).isoformat(),
        })
        settlements.append({
            "settlement_entry_id": "stl_501",
            "payout_id": "po_601",
            "transaction_id": "txn_301",
            "entry_type": "payment",
            "gross_minor": 500000,
            "fee_minor": 10000,
            "net_minor": 490000,
            "currency": "INR",
            "settled_at": (base_dt + datetime.timedelta(days=1, hours=4)).isoformat(),
        })

        # 2. Order with partial refund
        orders.append({
            "order_id": "ord_102",
            "amount_minor": 200000,
            "currency": "INR",
            "ordered_at": (base_dt + datetime.timedelta(hours=11)).isoformat(),
        })
        events.append({
            "event_id": "evt_202",
            "event_type": "payment_captured",
            "transaction_id": "txn_302",
            "order_id": "ord_102",
            "parent_payment_id": None,
            "amount_minor": 200000,
            "currency": "INR",
            "occurred_at": (base_dt + datetime.timedelta(hours=11, minutes=2)).isoformat(),
            "received_at": (base_dt + datetime.timedelta(hours=11, minutes=2, seconds=5)).isoformat(),
        })
        settlements.append({
            "settlement_entry_id": "stl_502",
            "payout_id": "po_601",
            "transaction_id": "txn_302",
            "entry_type": "payment",
            "gross_minor": 200000,
            "fee_minor": 4000,
            "net_minor": 196000,
            "currency": "INR",
            "settled_at": (base_dt + datetime.timedelta(days=1, hours=4)).isoformat(),
        })
        events.append({
            "event_id": "evt_203",
            "event_type": "refund_succeeded",
            "transaction_id": "rf_303",
            "order_id": None,
            "parent_payment_id": "txn_302",
            "amount_minor": 50000,
            "currency": "INR",
            "occurred_at": (base_dt + datetime.timedelta(hours=15)).isoformat(),
            "received_at": (base_dt + datetime.timedelta(hours=15, seconds=5)).isoformat(),
        })
        settlements.append({
            "settlement_entry_id": "stl_503",
            "payout_id": "po_601",
            "transaction_id": "rf_303",
            "entry_type": "refund",
            "gross_minor": -50000,
            "fee_minor": 0,
            "net_minor": -50000,
            "currency": "INR",
            "settled_at": (base_dt + datetime.timedelta(days=1, hours=4)).isoformat(),
        })

        # 3. In-flight transaction (Pending settlement)
        orders.append({
            "order_id": "ord_103",
            "amount_minor": 150000,
            "currency": "INR",
            "ordered_at": (base_dt + datetime.timedelta(days=2, hours=9)).isoformat(),
        })
        events.append({
            "event_id": "evt_204",
            "event_type": "payment_captured",
            "transaction_id": "txn_304",
            "order_id": "ord_103",
            "parent_payment_id": None,
            "amount_minor": 150000,
            "currency": "INR",
            "occurred_at": (base_dt + datetime.timedelta(days=2, hours=9, minutes=1)).isoformat(),
            "received_at": (base_dt + datetime.timedelta(days=2, hours=9, minutes=1, seconds=5)).isoformat(),
        })

        # 4. Undercharge amount mismatch exception
        orders.append({
            "order_id": "ord_104",
            "amount_minor": 300000,
            "currency": "INR",
            "ordered_at": (base_dt + datetime.timedelta(hours=12)).isoformat(),
        })
        events.append({
            "event_id": "evt_205",
            "event_type": "payment_captured",
            "transaction_id": "txn_305",
            "order_id": "ord_104",
            "parent_payment_id": None,
            "amount_minor": 250000,
            "currency": "INR",
            "occurred_at": (base_dt + datetime.timedelta(hours=12, minutes=1)).isoformat(),
            "received_at": (base_dt + datetime.timedelta(hours=12, minutes=1, seconds=5)).isoformat(),
        })

        # Summary payout for po_601 (490000 + 196000 - 50000 = 636000)
        payouts.append({
            "payout_id": "po_601",
            "amount_minor": 636000,
            "expected_entry_count": 3,
            "currency": "INR",
            "issued_at": (base_dt + datetime.timedelta(days=1, hours=6)).isoformat(),
        })

    # Write files
    orders_path = out / "orders.csv"
    with open(orders_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "amount_minor", "currency", "ordered_at"])
        writer.writeheader()
        writer.writerows(orders)

    events_path = out / "payment_events.jsonl"
    with open(events_path, "w", newline="\n", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    settlements_path = out / "settlement_entries.csv"
    with open(settlements_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "settlement_entry_id", "payout_id", "transaction_id", "entry_type",
                "gross_minor", "fee_minor", "net_minor", "currency", "settled_at"
            ]
        )
        writer.writeheader()
        writer.writerows(settlements)

    payouts_path = out / "payouts.csv"
    with open(payouts_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["payout_id", "amount_minor", "expected_entry_count", "currency", "issued_at"]
        )
        writer.writeheader()
        writer.writerows(payouts)

    # Compute actual hashes
    manifest_files = [
        {
            "filename": "orders.csv",
            "source_name": "orders",
            "record_count": len(orders),
            "sha256_hex": calculate_sha256(orders_path),
        },
        {
            "filename": "payment_events.jsonl",
            "source_name": "payment_events",
            "record_count": len(events),
            "sha256_hex": calculate_sha256(events_path),
        },
        {
            "filename": "settlement_entries.csv",
            "source_name": "settlement_entries",
            "record_count": len(settlements),
            "sha256_hex": calculate_sha256(settlements_path),
        },
        {
            "filename": "payouts.csv",
            "source_name": "payouts",
            "record_count": len(payouts),
            "sha256_hex": calculate_sha256(payouts_path),
        },
    ]

    manifest = {
        "schema_version": "1.0.0",
        "batch_id": batch_id,
        "scenario": scenario,
        "seed": seed,
        "base_time": base_time,
        "delivered_at": delivered_at.isoformat(),
        "files": manifest_files,
    }

    manifest_path = out / "manifest.json"
    with open(manifest_path, "w", newline="\n", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest
