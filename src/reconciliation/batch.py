"""Database-free, byte-preserving preparation of a manifest-declared batch.

Call prepare_batch before opening a write transaction. Consumers must use the
returned bytes/records, not reopen the input paths after validation.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
import csv
import datetime
import hashlib
import io
import json
from pathlib import Path

from reconciliation.contracts import (
    BatchManifest, OrderRecord, PaymentEventRecord, PayoutRecord,
    SettlementEntryRecord,
    parse_utc_timestamp,
)

CanonicalRecord = OrderRecord | PaymentEventRecord | SettlementEntryRecord | PayoutRecord
MODELS = {"orders": OrderRecord, "payment_events": PaymentEventRecord,
          "settlement_entries": SettlementEntryRecord, "payouts": PayoutRecord}


class BatchValidationError(ValueError):
    """Fatal batch structure/integrity failure; no rows may be ingested."""


@dataclass(frozen=True)
class BatchLimits:
    max_manifest_bytes: int = 1_048_576
    max_file_bytes: int = 16_777_216
    max_batch_bytes: int = 67_108_864
    max_row_bytes: int = 65_536
    max_rows_per_file: int = 100_000


DEFAULT_LIMITS = BatchLimits()


@dataclass(frozen=True)
class PreparedRow:
    line_number: int
    raw_text: str
    raw_bytes: bytes
    record: CanonicalRecord | None
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreparedFile:
    source_name: str
    filename: str
    file_hash: str
    raw_bytes: bytes
    rows: list[PreparedRow]


@dataclass(frozen=True)
class PreparedBatch:
    manifest: BatchManifest
    manifest_hash: str
    manifest_bytes: bytes
    files: list[PreparedFile]


def _read_file(root: Path, name: str, limit: int) -> bytes:
    path = root / name
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root):
            raise BatchValidationError(f"File escape outside batch directory: {name}")
        if not resolved.is_file():
            raise BatchValidationError(f"Required input is not a regular file: {name}")
        # Open the resolved target, not the originally supplied symlink.
        with resolved.open("rb") as handle:
            raw = handle.read(limit + 1)
        if len(raw) > limit:
            raise BatchValidationError(f"Input byte limit exceeded: {name}")
        return raw
    except (OSError, RuntimeError) as exc:
        raise BatchValidationError(f"Cannot read required file: {path.name}") from exc


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError(f"Nonstandard JSON constant: {value}")


def _json_object(text):
    data = json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    if not isinstance(data, dict):
        raise ValueError("JSON record must be an object")
    return data


def _row(source, number, raw, as_of, limits, data=None, error=None):
    if len(raw) > limits.max_row_bytes:
        raise BatchValidationError(f"Row byte limit exceeded at line {number}")
    # Invalid UTF-8 has no exact Unicode representation; bytes remain authoritative.
    text = raw.decode("utf-8", errors="replace")
    try:
        raw.decode("utf-8", errors="strict")
        if error:
            raise ValueError(error)
        if data is None:
            data = _json_object(text)
        record = MODELS[source].from_dict(data)
        if as_of is not None:
            future = [field.name for field in fields(record)
                      if isinstance(getattr(record, field.name), datetime.datetime)
                      and getattr(record, field.name) > as_of]
            if future:
                raise ValueError(f"Source timestamps exceed as_of: {', '.join(future)}")
        return PreparedRow(number, text, raw, record)
    except (ValueError, RecursionError) as exc:
        return PreparedRow(number, text, raw, None, (str(exc),))


def _parse_rows(source, raw, as_of, limits):
    if source == "payment_events":
        # Every LF-delimited physical line counts, including rejected blanks.
        rows = []
        for number, line in enumerate(io.BytesIO(raw), 1):
            if number > limits.max_rows_per_file:
                raise BatchValidationError("Row count limit exceeded")
            rows.append(_row(source, number, line, as_of, limits))
        return rows
    # newline='' recognizes CRLF/LF/CR without translating the evidence.
    lines = list(io.StringIO(raw.decode("utf-8", errors="surrogateescape"), newline=""))
    reader = csv.reader(lines, strict=True)
    rows = []
    try:
        header = next(reader, None)
        if header is None:
            raise BatchValidationError("Missing CSV header")
        header_bytes = "".join(lines[:reader.line_num]).encode("utf-8", errors="surrogateescape")
        if len(header_bytes) > limits.max_row_bytes:
            raise BatchValidationError("CSV header byte limit exceeded")
        header_bytes.decode("utf-8")
        required = {field.name for field in fields(MODELS[source])}
        if len(set(header)) != len(header) or not required.issubset(header):
            raise BatchValidationError("Duplicate or missing required CSV header")
        previous = reader.line_num
        for values in reader:
            if len(rows) >= limits.max_rows_per_file:
                raise BatchValidationError("Row count limit exceeded")
            row_bytes = "".join(lines[previous:reader.line_num]).encode("utf-8", errors="surrogateescape")
            error = "CSV row width does not match header" if len(values) != len(header) else None
            rows.append(_row(source, previous + 1, row_bytes, as_of, limits, dict(zip(header, values)), error))
            previous = reader.line_num
    except (csv.Error, UnicodeError) as exc:
        raise BatchValidationError(f"Invalid CSV structure/header: {exc}") from exc
    return rows


def prepare_batch(batch_dir: str | Path, as_of: str | datetime.datetime | None = None,
                  *, limits: BatchLimits = DEFAULT_LIMITS) -> PreparedBatch:
    """Validate all files before returning; malformed rows carry rejection errors.

    CSV counts exclude the mandatory header and count logical records (including
    blanks); JSONL counts every LF-delimited line, including blanks. Missing CSV
    headers, unrecoverable CSV syntax, integrity/containment/limit failures are
    fatal. Invalid UTF-8 rows retain exact raw_bytes and replacement raw_text.
    """
    for field in fields(limits):
        value = getattr(limits, field.name)
        if type(value) is not int or value <= 0:
            raise BatchValidationError(f"{field.name} must be a positive integer")
    try:
        root = Path(batch_dir).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise BatchValidationError("Cannot resolve batch directory") from exc
    manifest_bytes = _read_file(root, "manifest.json", min(limits.max_manifest_bytes, limits.max_batch_bytes))
    try:
        manifest = BatchManifest.from_dict(_json_object(manifest_bytes.decode("utf-8")))
        cutoff = parse_utc_timestamp(as_of, "as_of") if as_of is not None else None
        if cutoff is not None and manifest.delivered_at > cutoff:
            raise ValueError("Manifest delivered_at exceeds as_of")
    except (ValueError, RecursionError) as exc:
        raise BatchValidationError(f"Invalid manifest: {exc}") from exc
    files = []
    total_bytes = len(manifest_bytes)
    for entry in manifest.files:
        if entry.record_count > limits.max_rows_per_file:
            raise BatchValidationError(f"Row count limit exceeded: {entry.filename}")
        raw = _read_file(root, entry.filename, min(limits.max_file_bytes, limits.max_batch_bytes - total_bytes))
        total_bytes += len(raw)
        digest = hashlib.sha256(raw).hexdigest()
        if digest != entry.sha256_hex.lower():
            raise BatchValidationError(f"Hash mismatch: {entry.filename}")
        rows = _parse_rows(entry.source_name, raw, cutoff, limits)
        if len(rows) != entry.record_count:
            raise BatchValidationError(f"Record count mismatch: {entry.filename}")
        files.append(PreparedFile(entry.source_name, entry.filename, digest, raw, rows))
    return PreparedBatch(manifest, hashlib.sha256(manifest_bytes).hexdigest(), manifest_bytes, files)
