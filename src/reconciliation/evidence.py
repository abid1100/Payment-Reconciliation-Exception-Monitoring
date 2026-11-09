"""Durable exact-byte evidence archiving and provenance verification.

Archives immutable raw bytes (BYTEA) for manifest.json and declared source files
keyed by SHA-256, tracking delivery occurrences per batch without modifying
authoritative source bytes or duplicating identical payloads across deliveries.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any

from reconciliation.batch import (
    BatchLimits,
    BatchValidationError,
    DEFAULT_LIMITS,
    PreparedBatch,
    _json_object,
    _parse_rows,
)
from reconciliation.contracts import BatchManifest


class EvidenceError(Exception):
    """Base exception for evidence operations."""


class EvidenceUnavailableError(EvidenceError):
    """Raised when original exact-byte evidence was not archived (e.g. historical batch)."""


class EvidenceCorruptedError(EvidenceError):
    """Raised when stored evidence fails manifest verification, hash, length, or logical count integrity checks."""


@dataclass(frozen=True)
class ArchivedFileEvidence:
    source_type: str
    filename: str
    record_count: int
    file_hash: str
    raw_bytes: bytes


@dataclass(frozen=True)
class ArchivedBatchEvidence:
    batch_id: str
    manifest_hash: str
    manifest_bytes: bytes
    files: list[ArchivedFileEvidence]


def _ensure_blob(cur: Any, blob_hash: str, raw_bytes: bytes) -> None:
    """Insert or verify content-addressed blob in raw.evidence_blobs."""
    cur.execute(
        """
        SELECT byte_length, raw_bytes
        FROM raw.evidence_blobs
        WHERE blob_hash = %s;
        """,
        (blob_hash,),
    )
    existing = cur.fetchone()

    if existing is not None:
        stored_bytes = bytes(existing["raw_bytes"] if isinstance(existing, dict) else existing[1])
        stored_length = existing["byte_length"] if isinstance(existing, dict) else existing[0]

        if stored_length != len(raw_bytes) or stored_bytes != raw_bytes:
            raise ValueError(
                f"Corrupt or inconsistent evidence blob for hash '{blob_hash}': "
                f"stored length={stored_length}, input length={len(raw_bytes)}"
            )
        stored_hash = hashlib.sha256(stored_bytes).hexdigest()
        if stored_hash != blob_hash:
            raise ValueError(
                f"Corrupt evidence blob hash mismatch for '{blob_hash}': calculated '{stored_hash}'"
            )
    else:
        cur.execute(
            """
            INSERT INTO raw.evidence_blobs (blob_hash, byte_length, raw_bytes)
            VALUES (%s, %s, %s);
            """,
            (blob_hash, len(raw_bytes), raw_bytes),
        )


def stage_batch_evidence(conn: Any, batch: PreparedBatch) -> None:
    """Persist source batch, exact manifest/file blobs, and delivery associations.

    Executed in an explicit transaction before row staging and publication.
    On reuse of existing batches, verifies that content and associations match
    without duplicating physical blobs.
    """
    manifest = batch.manifest
    batch_id = manifest.batch_id

    with conn.cursor() as cur:
        # 1. Register or verify source_batches record
        cur.execute(
            "SELECT manifest_hash FROM raw.source_batches WHERE batch_id = %s;",
            (batch_id,),
        )
        existing_batch = cur.fetchone()
        if existing_batch is not None:
            existing_hash = existing_batch.get("manifest_hash") if isinstance(existing_batch, dict) else existing_batch[0]
            if existing_hash != batch.manifest_hash:
                raise ValueError(
                    f"Batch conflict: batch_id '{batch_id}' already ingested with different manifest "
                    f"hash '{existing_hash}' vs '{batch.manifest_hash}'."
                )
            cur.execute(
                "UPDATE raw.source_batches SET staged_at = CLOCK_TIMESTAMP() WHERE batch_id = %s;",
                (batch_id,),
            )
        else:
            cur.execute(
                """
                INSERT INTO raw.source_batches (batch_id, manifest_hash, schema_version, delivered_at)
                VALUES (%s, %s, %s, %s);
                """,
                (batch_id, batch.manifest_hash, manifest.schema_version, manifest.delivered_at),
            )

        # 2. Persist or verify manifest blob
        _ensure_blob(cur, batch.manifest_hash, batch.manifest_bytes)

        # 3. Associate batch with manifest blob
        cur.execute(
            "SELECT manifest_blob_hash FROM raw.batch_evidence WHERE batch_id = %s;",
            (batch_id,),
        )
        existing_m_ev = cur.fetchone()
        if existing_m_ev is not None:
            m_hash = existing_m_ev.get("manifest_blob_hash") if isinstance(existing_m_ev, dict) else existing_m_ev[0]
            if m_hash != batch.manifest_hash:
                raise ValueError(
                    f"Inconsistent manifest blob association for batch '{batch_id}': "
                    f"stored '{m_hash}' vs incoming '{batch.manifest_hash}'"
                )
        else:
            cur.execute(
                """
                INSERT INTO raw.batch_evidence (batch_id, manifest_blob_hash)
                VALUES (%s, %s);
                """,
                (batch_id, batch.manifest_hash),
            )

        # 4. Persist or verify each file blob and delivery association
        for prep_file in batch.files:
            source_type = prep_file.source_name
            file_hash = prep_file.file_hash
            raw_bytes = prep_file.raw_bytes
            rec_count = len(prep_file.rows)
            filename = prep_file.filename

            _ensure_blob(cur, file_hash, raw_bytes)

            cur.execute(
                """
                SELECT filename, record_count, file_blob_hash
                FROM raw.batch_file_evidence
                WHERE batch_id = %s AND source_type = %s;
                """,
                (batch_id, source_type),
            )
            existing_file_ev = cur.fetchone()
            if existing_file_ev is not None:
                e_fname = existing_file_ev.get("filename") if isinstance(existing_file_ev, dict) else existing_file_ev[0]
                e_rcount = existing_file_ev.get("record_count") if isinstance(existing_file_ev, dict) else existing_file_ev[1]
                e_fhash = existing_file_ev.get("file_blob_hash") if isinstance(existing_file_ev, dict) else existing_file_ev[2]

                if e_fname != filename or e_rcount != rec_count or e_fhash != file_hash:
                    raise ValueError(
                        f"Inconsistent file evidence association for batch '{batch_id}', source '{source_type}': "
                        f"stored ({e_fname}, {e_rcount}, {e_fhash}) vs incoming ({filename}, {rec_count}, {file_hash})"
                    )
            else:
                cur.execute(
                    """
                    INSERT INTO raw.batch_file_evidence (
                        batch_id, source_type, filename, record_count, file_blob_hash
                    ) VALUES (%s, %s, %s, %s, %s);
                    """,
                    (batch_id, source_type, filename, rec_count, file_hash),
                )


def read_batch_evidence(
    conn: Any,
    batch_id: str,
    *,
    limits: BatchLimits = DEFAULT_LIMITS,
) -> ArchivedBatchEvidence:
    """Retrieve and authoritatively verify exact-byte evidence for a delivered batch.

    Enforces BatchLimits on metadata and association counts before loading byte payloads.
    Verifies the COMPLETE batch against its archived manifest:
    - manifest batch identity matches requested batch_id;
    - exactly the declared source/file associations exist in the archive;
    - filenames, source types, hashes and declared counts agree with the manifest;
    - verifies stored blob hashes and lengths;
    - verifies logical record counts from archived bytes using existing parsing rules.

    Raises EvidenceUnavailableError for historical batches where original evidence was not archived.
    Raises EvidenceCorruptedError if stored evidence fails manifest verification or integrity checks.
    Raises BatchValidationError if configured batch or file size limits are exceeded.
    """
    with conn.cursor() as cur:
        # Step 1: Fetch manifest metadata (metadata only, bound size before loading payload)
        cur.execute(
            """
            SELECT be.manifest_blob_hash, eb.byte_length
            FROM raw.batch_evidence be
            JOIN raw.evidence_blobs eb ON eb.blob_hash = be.manifest_blob_hash
            WHERE be.batch_id = %s;
            """,
            (batch_id,),
        )
        m_meta = cur.fetchone()

        if m_meta is None:
            # Check if this batch exists historically in raw.source_batches without evidence
            cur.execute(
                "SELECT batch_id FROM raw.source_batches WHERE batch_id = %s;",
                (batch_id,),
            )
            hist = cur.fetchone()
            if hist is not None:
                raise EvidenceUnavailableError(
                    f"Original evidence unavailable for historical batch '{batch_id}': "
                    "batch was ingested before migration 0005_exact_byte_evidence.sql"
                )
            raise EvidenceUnavailableError(f"Batch '{batch_id}' not found in archive")

        m_hash = m_meta.get("manifest_blob_hash") if isinstance(m_meta, dict) else m_meta[0]
        m_len = m_meta.get("byte_length") if isinstance(m_meta, dict) else m_meta[1]

        if m_len > limits.max_manifest_bytes:
            raise BatchValidationError(
                f"Manifest byte limit exceeded in batch '{batch_id}': {m_len} > {limits.max_manifest_bytes}"
            )
        if m_len > limits.max_batch_bytes:
            raise BatchValidationError(
                f"Batch byte limit exceeded in batch '{batch_id}': {m_len} > {limits.max_batch_bytes}"
            )

        # Step 2: Fetch file associations metadata (metadata only, check counts & bounds)
        cur.execute(
            """
            SELECT
                bfe.source_type,
                bfe.filename,
                bfe.record_count,
                bfe.file_blob_hash,
                eb.byte_length
            FROM raw.batch_file_evidence bfe
            JOIN raw.evidence_blobs eb ON eb.blob_hash = bfe.file_blob_hash
            WHERE bfe.batch_id = %s
            ORDER BY bfe.source_type ASC;
            """,
            (batch_id,),
        )
        file_meta_rows = cur.fetchall() or []

        total_batch_bytes = m_len
        stored_meta_by_source: dict[str, dict[str, Any]] = {}
        for f_meta in file_meta_rows:
            f_dict = dict(f_meta) if isinstance(f_meta, dict) else {
                "source_type": f_meta[0],
                "filename": f_meta[1],
                "record_count": f_meta[2],
                "file_blob_hash": f_meta[3],
                "byte_length": f_meta[4],
            }
            st = f_dict["source_type"]
            f_len = f_dict["byte_length"]
            f_rcount = f_dict["record_count"]
            f_name = f_dict["filename"]

            if f_len > limits.max_file_bytes:
                raise BatchValidationError(
                    f"Input byte limit exceeded for file '{f_name}' in batch '{batch_id}': "
                    f"{f_len} > {limits.max_file_bytes}"
                )
            if f_rcount > limits.max_rows_per_file:
                raise BatchValidationError(
                    f"Row count limit exceeded for file '{f_name}' in batch '{batch_id}': "
                    f"{f_rcount} > {limits.max_rows_per_file}"
                )
            total_batch_bytes += f_len
            stored_meta_by_source[st] = f_dict

        if total_batch_bytes > limits.max_batch_bytes:
            raise BatchValidationError(
                f"Batch byte limit exceeded in batch '{batch_id}': {total_batch_bytes} > {limits.max_batch_bytes}"
            )

        # Step 3: Load manifest raw bytes and verify hash
        cur.execute(
            "SELECT raw_bytes FROM raw.evidence_blobs WHERE blob_hash = %s;",
            (m_hash,),
        )
        m_blob_row = cur.fetchone()
        if m_blob_row is None:
            raise EvidenceCorruptedError(f"Missing manifest blob '{m_hash}' for batch '{batch_id}'")
        m_bytes = bytes(m_blob_row.get("raw_bytes") if isinstance(m_blob_row, dict) else m_blob_row[0])

        if len(m_bytes) != m_len:
            raise EvidenceCorruptedError(
                f"Stored manifest byte length mismatch for batch '{batch_id}': expected {m_len}, got {len(m_bytes)}"
            )
        calc_m_hash = hashlib.sha256(m_bytes).hexdigest()
        if calc_m_hash != m_hash:
            raise EvidenceCorruptedError(
                f"Stored manifest hash mismatch for batch '{batch_id}': expected '{m_hash}', got '{calc_m_hash}'"
            )

        # Step 4: Parse manifest and verify batch identity
        try:
            manifest_dict = _json_object(m_bytes.decode("utf-8"))
            manifest = BatchManifest.from_dict(manifest_dict)
        except Exception as exc:
            raise EvidenceCorruptedError(f"Corrupt manifest JSON in batch '{batch_id}': {exc}") from exc

        if manifest.batch_id != batch_id:
            raise EvidenceCorruptedError(
                f"Archived manifest batch_id '{manifest.batch_id}' does not match requested batch '{batch_id}'"
            )

        # Step 5: Verify declared manifest files against stored associations
        manifest_files_by_source = {f.source_name: f for f in manifest.files}

        if set(manifest_files_by_source.keys()) != set(stored_meta_by_source.keys()):
            raise EvidenceCorruptedError(
                f"Mismatched file associations for batch '{batch_id}': manifest declares "
                f"{sorted(manifest_files_by_source.keys())}, archive has {sorted(stored_meta_by_source.keys())}"
            )

        for source_name, m_file in manifest_files_by_source.items():
            s_dict = stored_meta_by_source[source_name]
            if s_dict["filename"] != m_file.filename:
                raise EvidenceCorruptedError(
                    f"Filename mismatch for source '{source_name}' in batch '{batch_id}': "
                    f"manifest declared '{m_file.filename}', archive has '{s_dict['filename']}'"
                )
            if s_dict["record_count"] != m_file.record_count:
                raise EvidenceCorruptedError(
                    f"Record count mismatch for source '{source_name}' in batch '{batch_id}': "
                    f"manifest declared {m_file.record_count}, archive has {s_dict['record_count']}"
                )
            if s_dict["file_blob_hash"].lower() != m_file.sha256_hex.lower():
                raise EvidenceCorruptedError(
                    f"File hash mismatch for source '{source_name}' in batch '{batch_id}': "
                    f"manifest declared '{m_file.sha256_hex}', archive has '{s_dict['file_blob_hash']}'"
                )

        # Step 6: Load file byte payloads and verify bytes, hashes, and logical counts
        archived_files: list[ArchivedFileEvidence] = []
        for source_name in sorted(manifest_files_by_source.keys()):
            s_dict = stored_meta_by_source[source_name]
            f_hash = s_dict["file_blob_hash"]
            f_len = s_dict["byte_length"]
            f_name = s_dict["filename"]
            f_rcount = s_dict["record_count"]

            cur.execute(
                "SELECT raw_bytes FROM raw.evidence_blobs WHERE blob_hash = %s;",
                (f_hash,),
            )
            f_blob_row = cur.fetchone()
            if f_blob_row is None:
                raise EvidenceCorruptedError(
                    f"Missing file blob '{f_hash}' for source '{source_name}' in batch '{batch_id}'"
                )
            f_bytes = bytes(f_blob_row.get("raw_bytes") if isinstance(f_blob_row, dict) else f_blob_row[0])

            if len(f_bytes) != f_len:
                raise EvidenceCorruptedError(
                    f"Stored file byte length mismatch for batch '{batch_id}', source '{source_name}': "
                    f"expected {f_len}, got {len(f_bytes)}"
                )
            calc_fhash = hashlib.sha256(f_bytes).hexdigest()
            if calc_fhash.lower() != f_hash.lower():
                raise EvidenceCorruptedError(
                    f"Stored file hash mismatch for batch '{batch_id}', source '{source_name}': "
                    f"expected '{f_hash}', got '{calc_fhash}'"
                )

            # Verify logical record counts from archived bytes using existing parsing rules
            try:
                parsed_rows = _parse_rows(source_name, f_bytes, as_of=None, limits=limits)
                actual_count = len(parsed_rows)
            except Exception as exc:
                raise EvidenceCorruptedError(
                    f"Failed to parse logical records from archived bytes for source '{source_name}' "
                    f"in batch '{batch_id}': {exc}"
                ) from exc

            if actual_count != f_rcount:
                raise EvidenceCorruptedError(
                    f"Logical record count mismatch from archived bytes for source '{source_name}' "
                    f"in batch '{batch_id}': declared {f_rcount}, parsed {actual_count}"
                )

            archived_files.append(
                ArchivedFileEvidence(
                    source_type=source_name,
                    filename=f_name,
                    record_count=f_rcount,
                    file_hash=f_hash,
                    raw_bytes=f_bytes,
                )
            )

        return ArchivedBatchEvidence(
            batch_id=batch_id,
            manifest_hash=m_hash,
            manifest_bytes=m_bytes,
            files=archived_files,
        )
