"""PostgreSQL database connectivity, transaction context, and session advisory lock."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator, Iterator

logger = logging.getLogger(__name__)

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore


DEFAULT_APPLICATION_LOCK_ID = 428910001


def require_psycopg():
    if psycopg is None:
        raise ImportError(
            "psycopg is not installed. Install with `pip install 'psycopg[binary]>=3.1.18'` "
            "to connect to PostgreSQL."
        )


@contextmanager
def get_connection(dsn: str, autocommit: bool = False) -> Generator[Any, None, None]:
    """Provide a managed database connection with automatic commit on success and rollback on error."""
    require_psycopg()
    conn = psycopg.connect(dsn, autocommit=autocommit, row_factory=dict_row)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit and not conn.closed:
            conn.rollback()
        raise
    finally:
        if not conn.closed:
            conn.close()


@contextmanager
def pipeline_session_lock(dsn: str, lock_id: int = DEFAULT_APPLICATION_LOCK_ID) -> Generator[Any, None, None]:
    """Hold a session-scoped PostgreSQL advisory lock across the pipeline lifecycle.
    
    If the process crashes or connection is dropped, PostgreSQL immediately auto-releases
    the session lock upon TCP socket termination.
    """
    require_psycopg()
    conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
    lock_acquired = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s) AS acquired;", (lock_id,))
            res = cur.fetchone()
            if not res or not res["acquired"]:
                raise RuntimeError(
                    f"Pipeline aborted: Another writer or migration holds advisory lock ID {lock_id}."
                )
            lock_acquired = True
            logger.info("Acquired pipeline advisory lock %s", lock_id)

        yield conn

    finally:
        if lock_acquired and not conn.closed:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (lock_id,))
                logger.info("Released pipeline advisory lock %s", lock_id)
            except Exception as e:
                logger.warning("Error releasing advisory lock: %s", e)
        if not conn.closed:
            conn.close()
