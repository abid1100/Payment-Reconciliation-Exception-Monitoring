"""Typed contracts, strict monetary/timestamp validation, and canonical models."""
from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from typing import Any, Optional


def parse_utc_timestamp(value: Any, field_name: str) -> datetime.datetime:
    """Parse ISO-8601 string to timezone-aware UTC datetime.
    
    Rejects naive timestamps without explicit timezone.
    """
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field_name} must be timezone-aware.")
        return value.astimezone(datetime.timezone.utc)

    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})", value
    ):
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 timestamp string.")

    cleaned = value
    try:
        # Support trailing 'Z'
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(cleaned)
    except Exception as e:
        raise ValueError(f"Invalid ISO-8601 format for {field_name}: '{value}'. Reason: {e}") from e

    if dt.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information (e.g. 'Z' or '+00:00').")

    return dt.astimezone(datetime.timezone.utc)


def parse_integer_minor(value: Any, field_name: str, allow_negative: bool = False) -> int:
    """Validate and parse integer minor units (e.g. paise / cents).
    
    Strictly rejects floats, booleans, fractional strings, and NaN.
    """
    # In Python, bool is a subclass of int (True == 1), so reject explicitly
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer, got boolean: {value}")

    if isinstance(value, float):
        raise ValueError(f"{field_name} floating-point values are rejected to prevent precision loss: {value}")

    if isinstance(value, str):
        cleaned = value
        if not re.fullmatch(r"[+-]?[0-9]+", cleaned):
            raise ValueError(f"{field_name} must be an ordinary signed decimal integer string")
        try:
            val = int(cleaned)
        except ValueError as e:
            raise ValueError(f"{field_name} must be a valid integer string: '{value}'") from e
    elif isinstance(value, int):
        val = value
    else:
        raise ValueError(f"{field_name} must be an integer, got {type(value).__name__}: {value}")

    if not -(2**63) <= val <= 2**63 - 1:
        raise ValueError(f"{field_name} must fit signed BIGINT")
    if not allow_negative and val < 0:
        raise ValueError(f"{field_name} must be non-negative, got: {val}")

    return val


def validate_identifier(value: Any, field_name: str) -> str:
    """Validate opaque string identifier."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
        raise ValueError(f"{field_name} must be an ASCII identifier of 1 to 64 characters")
    return value


def validate_currency(value: Any, field_name: str, expected_currency: str = "INR") -> str:
    """Validate ISO-4217 currency code."""
    if not isinstance(value, str) or value != expected_currency or expected_currency != "INR":
        raise ValueError(f"{field_name} must be '{expected_currency}', got '{value}'")
    return expected_currency


# =====================================================================
# Canonical Record Models
# =====================================================================

@dataclass(frozen=True)
class OrderRecord:
    order_id: str
    amount_minor: int
    currency: str
    ordered_at: datetime.datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OrderRecord:
        order_id = validate_identifier(data.get("order_id"), "order_id")
        amount = parse_integer_minor(data.get("amount_minor"), "amount_minor", allow_negative=False)
        if amount <= 0:
            raise ValueError(f"order amount_minor must be positive (>0), got: {amount}")
        currency = validate_currency(data.get("currency"), "currency")
        ordered_at = parse_utc_timestamp(data.get("ordered_at"), "ordered_at")
        return cls(order_id=order_id, amount_minor=amount, currency=currency, ordered_at=ordered_at)

    def business_payload(self) -> dict[str, Any]:
        return {"amount_minor": self.amount_minor, "currency": self.currency, "ordered_at": self.ordered_at.isoformat()}


@dataclass(frozen=True)
class PaymentEventRecord:
    event_id: str
    event_type: str
    transaction_id: str
    order_id: Optional[str]
    parent_payment_id: Optional[str]
    amount_minor: int
    currency: str
    occurred_at: datetime.datetime
    received_at: datetime.datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PaymentEventRecord:
        event_id = validate_identifier(data.get("event_id"), "event_id")
        event_type = data.get("event_type")
        if event_type not in ("payment_captured", "refund_succeeded"):
            raise ValueError(f"event_type must be 'payment_captured' or 'refund_succeeded', got: '{event_type}'")

        txn_id = validate_identifier(data.get("transaction_id"), "transaction_id")
        amount = parse_integer_minor(data.get("amount_minor"), "amount_minor", allow_negative=False)
        if amount <= 0:
            raise ValueError(f"event amount_minor magnitude must be positive (>0), got: {amount}")

        currency = validate_currency(data.get("currency"), "currency")
        occurred_at = parse_utc_timestamp(data.get("occurred_at"), "occurred_at")
        received_at = parse_utc_timestamp(data.get("received_at"), "received_at")

        if received_at < occurred_at:
            raise ValueError(f"received_at ({received_at}) cannot precede occurred_at ({occurred_at})")

        order_id: Optional[str] = None
        parent_payment_id: Optional[str] = None

        if event_type == "payment_captured":
            order_id = validate_identifier(data.get("order_id"), "order_id")
            if data.get("parent_payment_id") is not None:
                raise ValueError("parent_payment_id must be null for payment_captured event")
        elif event_type == "refund_succeeded":
            parent_payment_id = validate_identifier(data.get("parent_payment_id"), "parent_payment_id")
            if data.get("order_id") is not None:
                raise ValueError("order_id must be null for refund_succeeded event")

        return cls(
            event_id=event_id,
            event_type=event_type,
            transaction_id=txn_id,
            order_id=order_id,
            parent_payment_id=parent_payment_id,
            amount_minor=amount,
            currency=currency,
            occurred_at=occurred_at,
            received_at=received_at,
        )

    def business_payload(self) -> dict[str, Any]:
        """Business equality fields (excludes delivery received_at)."""
        return {
            "event_type": self.event_type,
            "transaction_id": self.transaction_id,
            "order_id": self.order_id,
            "parent_payment_id": self.parent_payment_id,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "occurred_at": self.occurred_at.isoformat(),
        }


@dataclass(frozen=True)
class SettlementEntryRecord:
    settlement_entry_id: str
    payout_id: str
    transaction_id: str
    entry_type: str
    gross_minor: int
    fee_minor: int
    net_minor: int
    currency: str
    settled_at: datetime.datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SettlementEntryRecord:
        entry_id = validate_identifier(data.get("settlement_entry_id"), "settlement_entry_id")
        payout_id = validate_identifier(data.get("payout_id"), "payout_id")
        txn_id = validate_identifier(data.get("transaction_id"), "transaction_id")
        entry_type = data.get("entry_type")
        if entry_type not in ("payment", "refund"):
            raise ValueError(f"entry_type must be 'payment' or 'refund', got: '{entry_type}'")

        gross = parse_integer_minor(data.get("gross_minor"), "gross_minor", allow_negative=True)
        fee = parse_integer_minor(data.get("fee_minor"), "fee_minor", allow_negative=False)
        net = parse_integer_minor(data.get("net_minor"), "net_minor", allow_negative=True)
        currency = validate_currency(data.get("currency"), "currency")
        settled_at = parse_utc_timestamp(data.get("settled_at"), "settled_at")

        # Structural sign rules
        if entry_type == "payment" and gross <= 0:
            raise ValueError(f"payment settlement line gross must be positive (>0), got: {gross}")
        if entry_type == "refund" and gross >= 0:
            raise ValueError(f"refund settlement line gross must be negative (<0), got: {gross}")
        if entry_type == "refund" and fee != 0:
            raise ValueError("refund settlement fee_minor must be zero")

        return cls(
            settlement_entry_id=entry_id,
            payout_id=payout_id,
            transaction_id=txn_id,
            entry_type=entry_type,
            gross_minor=gross,
            fee_minor=fee,
            net_minor=net,
            currency=currency,
            settled_at=settled_at,
        )

    def business_payload(self) -> dict[str, Any]:
        return {
            "payout_id": self.payout_id,
            "transaction_id": self.transaction_id,
            "entry_type": self.entry_type,
            "gross_minor": self.gross_minor,
            "fee_minor": self.fee_minor,
            "net_minor": self.net_minor,
            "currency": self.currency,
            "settled_at": self.settled_at.isoformat(),
        }


@dataclass(frozen=True)
class PayoutRecord:
    payout_id: str
    amount_minor: int
    expected_entry_count: int
    currency: str
    issued_at: datetime.datetime

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PayoutRecord:
        payout_id = validate_identifier(data.get("payout_id"), "payout_id")
        amount = parse_integer_minor(data.get("amount_minor"), "amount_minor", allow_negative=True)
        expected_count = parse_integer_minor(data.get("expected_entry_count"), "expected_entry_count", allow_negative=False)
        if expected_count > 2**31 - 1:
            raise ValueError("expected_entry_count must fit signed INTEGER")
        currency = validate_currency(data.get("currency"), "currency")
        issued_at = parse_utc_timestamp(data.get("issued_at"), "issued_at")
        return cls(
            payout_id=payout_id,
            amount_minor=amount,
            expected_entry_count=expected_count,
            currency=currency,
            issued_at=issued_at,
        )

    def business_payload(self) -> dict[str, Any]:
        return {
            "amount_minor": self.amount_minor,
            "expected_entry_count": self.expected_entry_count,
            "currency": self.currency,
            "issued_at": self.issued_at.isoformat(),
        }


@dataclass(frozen=True)
class ManifestFileEntry:
    filename: str
    source_name: str
    record_count: int
    sha256_hex: str


@dataclass(frozen=True)
class BatchManifest:
    schema_version: str
    batch_id: str
    scenario: str
    seed: int
    base_time: datetime.datetime
    delivered_at: datetime.datetime
    files: list[ManifestFileEntry]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BatchManifest:
        if not isinstance(data, dict):
            raise ValueError("Manifest must be an object")
        schema_version = data.get("schema_version")
        if schema_version != "1.0.0":
            raise ValueError(f"Unsupported schema_version: '{schema_version}'. Expected '1.0.0'.")

        batch_id = validate_identifier(data.get("batch_id"), "batch_id")
        scenario = validate_identifier(data.get("scenario"), "scenario")
        seed = data.get("seed")
        if type(seed) is not int:
            raise ValueError("Manifest seed must be an integer")
        base_time = parse_utc_timestamp(data.get("base_time"), "base_time")
        delivered_at = parse_utc_timestamp(data.get("delivered_at"), "delivered_at")
        if base_time > delivered_at:
            raise ValueError("Manifest base_time cannot exceed delivered_at")

        raw_files = data.get("files")
        if not isinstance(raw_files, list) or len(raw_files) == 0:
            raise ValueError("Manifest must contain a non-empty 'files' array.")

        files: list[ManifestFileEntry] = []
        filenames: set[str] = set()
        sources: set[str] = set()
        for f in raw_files:
            if not isinstance(f, dict):
                raise ValueError("Manifest file entry must be an object")
            fn = validate_identifier(f.get("filename"), "filename")
            # Path traversal prevention
            if "/" in fn or "\\" in fn or ".." in fn or fn.endswith(".") or re.fullmatch(
                r"(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?", fn, flags=re.IGNORECASE
            ):
                raise ValueError(f"Path traversal characters forbidden in manifest filename: '{fn}'")

            sn = f.get("source_name")
            if sn not in ("orders", "payment_events", "settlement_entries", "payouts"):
                raise ValueError("Unknown manifest source_name")
            # Case-insensitive file uniqueness is portable to Windows filesystems;
            # business IDs and source names themselves remain case-sensitive.
            if fn.casefold() in filenames or sn in sources:
                raise ValueError("Manifest filenames and sources must be unique")
            filenames.add(fn.casefold())
            sources.add(sn)
            rc = f.get("record_count")
            if type(rc) is not int or rc < 0:
                raise ValueError("Manifest record_count must be a nonnegative integer")
            h = f.get("sha256_hex")
            if not isinstance(h, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", h):
                raise ValueError(f"Invalid SHA-256 hex digest for file '{fn}'")
            files.append(ManifestFileEntry(filename=fn, source_name=sn, record_count=rc, sha256_hex=h))

        return cls(
            schema_version=schema_version,
            batch_id=batch_id,
            scenario=scenario,
            seed=seed,
            base_time=base_time,
            delivered_at=delivered_at,
            files=files,
        )
