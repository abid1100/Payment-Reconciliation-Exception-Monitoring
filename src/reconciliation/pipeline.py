"""Atomic pipeline execution, run lifecycle orchestration, and failure isolation."""
from __future__ import annotations

import datetime
import json
import logging
import uuid
from pathlib import Path
from typing import Any

from reconciliation.config import AppConfig
from reconciliation.contracts import parse_utc_timestamp
from reconciliation.database import require_psycopg, pipeline_session_lock
from reconciliation.ingest import stage_raw_batch_evidence, load_canonical_facts
from reconciliation.reconcile import evaluate_reconciliation

logger = logging.getLogger(__name__)


def verify_connection_identity(conn: Any, expected_role: str = "pipeline", is_test: bool = False) -> None:
    """Verify PostgreSQL current_database() and current_user match the expected environment and role.

    Fails closed if the identity cannot be established, returns empty results, or mismatches the expected target/role.
    """
    expected_db = "recon_test" if is_test else "recon_dev"
    prefix = "recon_test" if is_test else "recon"
    user_suffix = {"migration": "owner", "pipeline": "pipeline", "dashboard": "dashboard"}.get(expected_role, expected_role)
    expected_user = f"{prefix}_{user_suffix}"

    with conn.cursor() as cur:
        cur.execute("SELECT current_database() AS dbname, current_user AS username;")
        row = cur.fetchone()
        if not row:
            raise ValueError("Connection identity check failed: empty result from identity query")
        actual_db = row["dbname"] if isinstance(row, dict) else row[0]
        actual_user = row["username"] if isinstance(row, dict) else row[1]
        if actual_db != expected_db:
            raise ValueError(f"Database target mismatch: expected '{expected_db}', connected to '{actual_db}'")
        if actual_user != expected_user:
            raise ValueError(f"Database user mismatch: expected '{expected_user}', connected as '{actual_user}'")


def inspect_unfinished_runs(conn: Any) -> list[dict[str, Any]]:
    """Inspect and return all pipeline runs currently in RUNNING state under the writer lock."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, run_sequence, as_of, config_version, rule_version, started_at, input_summary
            FROM raw.pipeline_runs
            WHERE status = 'RUNNING'
            ORDER BY run_sequence ASC;
            """
        )
        rows = cur.fetchall()
        results = []
        for row in rows:
            if isinstance(row, dict):
                results.append(row)
            else:
                results.append({
                    "run_id": row[0],
                    "run_sequence": row[1],
                    "as_of": row[2],
                    "config_version": row[3],
                    "rule_version": row[4],
                    "started_at": row[5],
                    "input_summary": row[6],
                })
        return results


def mark_run_interrupted(conn: Any, run_id: str, reason: str = "Process interrupted before completion") -> None:
    """Transition an unfinished RUNNING run into terminal INTERRUPTED state with finished_at set."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw.pipeline_runs
            SET status = 'INTERRUPTED',
                finished_at = CLOCK_TIMESTAMP(),
                error_summary = %s
            WHERE run_id = %s AND status = 'RUNNING';
            """,
            (reason, run_id),
        )


def run_pipeline_batch(
    input_dir: str | Path,
    as_of_time: str | datetime.datetime,
    config: AppConfig,
    dsn: str,
    is_test: bool = False,
) -> dict[str, Any]:
    """Execute complete reconciliation run lifecycle with atomic commit/rollback.

    Stages raw evidence before canonical writes. Canonical writes, results, and SUCCEEDED
    status commit atomically. On business transaction failure, rolls back canonical writes
    while preserving raw evidence in raw.raw_records.
    """
    require_psycopg()
    as_of_dt = parse_utc_timestamp(as_of_time, "as_of")

    # 1. Strictly validate and prepare batch BEFORE opening database writer lock/transactions.
    # Fatal manifest/file integrity/hash/path/time violations raise BatchValidationError immediately.
    from reconciliation.batch import prepare_batch
    prepared = prepare_batch(input_dir, as_of=as_of_dt)

    run_id = f"run_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # 2. Acquire session-scoped advisory lock (enforces single active writer)
    with pipeline_session_lock(dsn, lock_id=config.policy.application_lock_id) as conn:
        # Step 0: Verify actual connection identity/target
        verify_connection_identity(conn, expected_role="pipeline", is_test=is_test)

        # Step 1: Enforce non-decreasing evaluation times under the writer lock
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(as_of) AS latest_as_of FROM raw.pipeline_runs WHERE status = 'SUCCEEDED';"
            )
            latest_row = cur.fetchone()
            if latest_row:
                latest_as_of = latest_row["latest_as_of"] if isinstance(latest_row, dict) else latest_row[0]
                if latest_as_of is not None and as_of_dt < latest_as_of:
                    raise ValueError(
                        f"Evaluation time '{as_of_dt.isoformat()}' is earlier than latest succeeded run "
                        f"evaluation time '{latest_as_of.isoformat()}'; non-decreasing evaluation times are strictly enforced."
                    )

        # Step 2: Register initial RUNNING run in raw.pipeline_runs (committed)
        input_identity = {
            "batch_id": prepared.manifest.batch_id,
            "manifest_hash": prepared.manifest_hash,
            "delivered_at": prepared.manifest.delivered_at.isoformat(),
            "files": [
                {
                    "source_type": f.source_name,
                    "filename": f.filename,
                    "file_hash": f.file_hash,
                }
                for f in prepared.files
            ],
        }
        initial_input_summary = {
            "policy": config.policy.to_snapshot_dict(),
            "input": input_identity,
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.pipeline_runs (run_id, status, as_of, config_version, rule_version, input_summary)
                VALUES (%s, 'RUNNING', %s, %s, %s, %s)
                RETURNING run_sequence;
                """,
                (
                    run_id,
                    as_of_dt,
                    config.policy.config_version,
                    config.policy.rule_version,
                    json.dumps(initial_input_summary),
                ),
            )
            run_sequence = cur.fetchone()["run_sequence"]

        # Step 3: Stage raw evidence in raw.source_batches and raw.raw_records BEFORE canonical transaction
        ingest_summary = None
        try:
            ingest_summary, accepted_records = stage_raw_batch_evidence(
                conn=conn,
                batch=prepared,
            )
        except Exception as e:
            logger.error("Raw evidence staging failed for run %s: %s", run_id, e)
            failed_summary = {
                "policy": config.policy.to_snapshot_dict(),
                "input": input_identity,
                "ingestion": ingest_summary.to_dict() if ingest_summary else None,
            }
            try:
                if hasattr(conn, "transaction"):
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE raw.pipeline_runs
                                SET status = 'FAILED',
                                    finished_at = CLOCK_TIMESTAMP(),
                                    error_summary = %s,
                                    input_summary = %s
                                WHERE run_id = %s;
                                """,
                                (str(e), json.dumps(failed_summary), run_id),
                            )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE raw.pipeline_runs
                            SET status = 'FAILED',
                                finished_at = CLOCK_TIMESTAMP(),
                                error_summary = %s,
                                input_summary = %s
                            WHERE run_id = %s;
                            """,
                            (str(e), json.dumps(failed_summary), run_id),
                        )
            except Exception as log_err:
                logger.critical("Could not record FAILED state in pipeline_runs: %s", log_err)
            return {
                "run_id": run_id,
                "run_sequence": run_sequence,
                "status": "FAILED",
                "error": str(e),
            }

        # Step 4: Atomic Business Transaction
        # Canonical facts, reconciliation results, dispositions, processed markers, and SUCCEEDED commit atomically.
        try:
            with conn.transaction():
                # Persist evaluated dispositions for this run attempt
                with conn.cursor() as cur:
                    for disp in ingest_summary.dispositions:
                        cur.execute(
                            """
                            INSERT INTO raw.record_dispositions (
                                run_id, raw_record_id, disposition, rejection_reason, canonical_id
                            ) VALUES (%s, %s, %s, %s, %s);
                            """,
                            (run_id, disp.raw_record_id, disp.disposition, disp.rejection_reason, disp.canonical_id),
                        )

                # Load accepted canonical facts
                load_canonical_facts(
                    conn=conn,
                    accepted_records=accepted_records,
                    run_sequence=run_sequence,
                )

                # Reconcile all rules at evaluation time
                results = evaluate_reconciliation(
                    conn=conn,
                    run_id=run_id,
                    as_of=as_of_dt,
                    grace_period_hours=config.policy.grace_period_hours,
                    max_run_sequence=run_sequence,
                )

                # Atomically publish results snapshot
                with conn.cursor() as cur:
                    for res in results:
                        cur.execute(
                            """
                            INSERT INTO reporting.reconciliation_results (
                                run_id, rule_id, subject_type, subject_id, state, reason_code,
                                expected_value, observed_value, difference_minor, due_at, evidence, evaluated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            res.to_tuple(),
                        )

                    # Update run status to SUCCEEDED and commit authoritative processed markers + policy snapshot
                    published_files = [
                        {"source_type": f.source_name, "file_hash": f.file_hash}
                        for f in prepared.files
                    ]
                    combined_summary = {
                        "policy": config.policy.to_snapshot_dict(),
                        "input": input_identity,
                        "ingestion": ingest_summary.to_dict(),
                        "processed_batches": [prepared.manifest.batch_id],
                        "markers": {
                            "batch_id": prepared.manifest.batch_id,
                            "manifest_hash": prepared.manifest_hash,
                            "delivered_at": prepared.manifest.delivered_at.isoformat(),
                            "files_processed": ingest_summary.files_processed,
                            "files_skipped": ingest_summary.files_skipped,
                            "published_files": published_files,
                        },
                    }
                    cur.execute(
                        """
                        UPDATE raw.pipeline_runs
                        SET status = 'SUCCEEDED',
                            finished_at = CLOCK_TIMESTAMP(),
                            input_summary = %s
                        WHERE run_id = %s;
                        """,
                        (json.dumps(combined_summary), run_id),
                    )

            logger.info("Run %s SUCCEEDED! Reconciled %d rules.", run_id, len(results))
            return {
                "run_id": run_id,
                "run_sequence": run_sequence,
                "status": "SUCCEEDED",
                "rules_evaluated": len(results),
                "summary": ingest_summary.to_dict(),
            }

        except Exception as e:
            logger.error("Run %s FAILED: %s. Rolling back canonical writes.", run_id, e)
            failed_summary = {
                "policy": config.policy.to_snapshot_dict(),
                "input": input_identity,
                "ingestion": ingest_summary.to_dict() if ingest_summary else None,
            }
            # Record FAILED in a separate transaction after the aborted transaction has rolled back
            try:
                if hasattr(conn, "transaction"):
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE raw.pipeline_runs
                                SET status = 'FAILED',
                                    finished_at = CLOCK_TIMESTAMP(),
                                    error_summary = %s,
                                    input_summary = %s
                                WHERE run_id = %s;
                                """,
                                (str(e), json.dumps(failed_summary), run_id),
                            )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE raw.pipeline_runs
                            SET status = 'FAILED',
                                finished_at = CLOCK_TIMESTAMP(),
                                error_summary = %s,
                                input_summary = %s
                            WHERE run_id = %s;
                            """,
                            (str(e), json.dumps(failed_summary), run_id),
                        )
            except Exception as log_err:
                logger.critical("Could not record FAILED state in pipeline_runs: %s", log_err)

            return {
                "run_id": run_id,
                "run_sequence": run_sequence,
                "status": "FAILED",
                "error": str(e),
            }


def evaluate_existing_state(
    as_of_time: str | datetime.datetime,
    config: AppConfig,
    dsn: str,
    is_test: bool = False,
) -> dict[str, Any]:
    """Re-evaluate existing canonical state when time advances without new batch ingestion."""
    require_psycopg()
    as_of_dt = parse_utc_timestamp(as_of_time, "as_of")
    run_id = f"eval_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    with pipeline_session_lock(dsn, lock_id=config.policy.application_lock_id) as conn:
        verify_connection_identity(conn, expected_role="pipeline", is_test=is_test)

        # Enforce non-decreasing evaluation times
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(as_of) AS latest_as_of FROM raw.pipeline_runs WHERE status = 'SUCCEEDED';"
            )
            latest_row = cur.fetchone()
            if latest_row:
                latest_as_of = latest_row["latest_as_of"] if isinstance(latest_row, dict) else latest_row[0]
                if latest_as_of is not None and as_of_dt < latest_as_of:
                    raise ValueError(
                        f"Evaluation time '{as_of_dt.isoformat()}' is earlier than latest succeeded run "
                        f"evaluation time '{latest_as_of.isoformat()}'; non-decreasing evaluation times are strictly enforced."
                    )

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.pipeline_runs (run_id, status, as_of, config_version, rule_version, input_summary)
                VALUES (%s, 'RUNNING', %s, %s, %s, %s)
                RETURNING run_sequence;
                """,
                (
                    run_id,
                    as_of_dt,
                    config.policy.config_version,
                    config.policy.rule_version,
                    json.dumps({
                        "action": "evaluate_only",
                        "policy": config.policy.to_snapshot_dict(),
                    }),
                ),
            )
            run_sequence = cur.fetchone()["run_sequence"]

        try:
            with conn.transaction():
                results = evaluate_reconciliation(
                    conn=conn,
                    run_id=run_id,
                    as_of=as_of_dt,
                    grace_period_hours=config.policy.grace_period_hours,
                    max_run_sequence=run_sequence,
                )

                with conn.cursor() as cur:
                    for res in results:
                        cur.execute(
                            """
                            INSERT INTO reporting.reconciliation_results (
                                run_id, rule_id, subject_type, subject_id, state, reason_code,
                                expected_value, observed_value, difference_minor, due_at, evidence, evaluated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """,
                            res.to_tuple(),
                        )

                    cur.execute(
                        """
                        UPDATE raw.pipeline_runs
                        SET status = 'SUCCEEDED', finished_at = CLOCK_TIMESTAMP()
                        WHERE run_id = %s;
                        """,
                        (run_id,),
                    )

            return {
                "run_id": run_id,
                "run_sequence": run_sequence,
                "status": "SUCCEEDED",
                "rules_evaluated": len(results),
            }
        except Exception as e:
            try:
                if hasattr(conn, "transaction"):
                    with conn.transaction():
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE raw.pipeline_runs SET status = 'FAILED', error_summary = %s, finished_at = CLOCK_TIMESTAMP() WHERE run_id = %s;",
                                (str(e), run_id),
                            )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE raw.pipeline_runs SET status = 'FAILED', error_summary = %s, finished_at = CLOCK_TIMESTAMP() WHERE run_id = %s;",
                            (str(e), run_id),
                        )
            except Exception as log_err:
                logger.critical("Could not record FAILED state in pipeline_runs: %s", log_err)
            return {"run_id": run_id, "status": "FAILED", "error": str(e)}
