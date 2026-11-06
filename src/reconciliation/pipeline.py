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
from reconciliation.ingest import ingest_batch_into_database
from reconciliation.reconcile import evaluate_reconciliation

logger = logging.getLogger(__name__)


def run_pipeline_batch(
    input_dir: str | Path,
    as_of_time: str | datetime.datetime,
    config: AppConfig,
    dsn: str,
) -> dict[str, Any]:
    """Execute complete reconciliation run lifecycle with atomic commit/rollback."""
    require_psycopg()
    as_of_dt = parse_utc_timestamp(as_of_time, "as_of")
    run_id = f"run_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # 1. Acquire session-scoped advisory lock (enforces single active writer)
    with pipeline_session_lock(dsn, lock_id=config.policy.application_lock_id) as conn:
        # Step A: Register RUNNING status in raw.pipeline_runs
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
                    json.dumps(config.policy.to_snapshot_dict()),
                ),
            )
            run_sequence = cur.fetchone()["run_sequence"]

        # Step B: Execute ingestion + reconciliation + atomic publication inside a single transaction
        try:
            with conn.transaction():
                # Ingest raw and core facts
                ingest_summary = ingest_batch_into_database(
                    conn=conn,
                    batch_dir=input_dir,
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

                    # Update run status to SUCCEEDED
                    cur.execute(
                        """
                        UPDATE raw.pipeline_runs
                        SET status = 'SUCCEEDED',
                            finished_at = CLOCK_TIMESTAMP(),
                            input_summary = %s
                        WHERE run_id = %s;
                        """,
                        (json.dumps(ingest_summary.to_dict()), run_id),
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
            # Record FAILED in a separate transaction
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE raw.pipeline_runs
                        SET status = 'FAILED',
                            finished_at = CLOCK_TIMESTAMP(),
                            error_summary = %s
                        WHERE run_id = %s;
                        """,
                        (str(e), run_id),
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
) -> dict[str, Any]:
    """Re-evaluate existing canonical state when time advances without new batch ingestion."""
    require_psycopg()
    as_of_dt = parse_utc_timestamp(as_of_time, "as_of")
    run_id = f"eval_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    with pipeline_session_lock(dsn, lock_id=config.policy.application_lock_id) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw.pipeline_runs (run_id, status, as_of, config_version, rule_version, input_summary)
                VALUES (%s, 'RUNNING', %s, %s, %s, '{"action": "evaluate_only"}'::jsonb)
                RETURNING run_sequence;
                """,
                (run_id, as_of_dt, config.policy.config_version, config.policy.rule_version),
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
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE raw.pipeline_runs SET status = 'FAILED', error_summary = %s, finished_at = CLOCK_TIMESTAMP() WHERE run_id = %s;",
                    (str(e), run_id),
                )
            return {"run_id": run_id, "status": "FAILED", "error": str(e)}
