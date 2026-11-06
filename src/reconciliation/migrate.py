"""Versioned, checksummed SQL migration runner."""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from reconciliation.database import require_psycopg, pipeline_session_lock

logger = logging.getLogger(__name__)


def calculate_migration_checksum(content: str) -> str:
    """Compute SHA-256 checksum of SQL migration content."""
    return hashlib.sha256(content.strip().encode("utf-8")).hexdigest().lower()


def get_migrations_dir() -> Path:
    """Resolve migrations directory packaged inside reconciliation module."""
    base = Path(__file__).resolve().parent / "sql" / "migrations"
    if not base.is_dir():
        raise FileNotFoundError(f"Migrations directory not found at: {base}")
    return base


def run_migrations(dsn: str, lock_id: int = 428910001) -> int:
    """Execute unapplied migrations in order, verifying checksum integrity."""
    require_psycopg()
    migrations_dir = get_migrations_dir()
    sql_files = sorted(migrations_dir.glob("*.sql"))

    if not sql_files:
        logger.info("No migration files found.")
        return 0

    applied_count = 0

    # Acquire session advisory lock during migrations
    with pipeline_session_lock(dsn, lock_id=lock_id) as conn:
        with conn.cursor() as cur:
            # Ensure schema and bootstrap table exist
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw.schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(128) NOT NULL,
                    checksum CHAR(64) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
                );
            """)

            # Fetch existing applied migrations
            cur.execute("SELECT version, name, checksum FROM raw.schema_migrations ORDER BY version ASC;")
            applied = {row["version"]: row for row in cur.fetchall()}

        for sql_file in sql_files:
            # Format: 0001_initial.sql -> version = 1
            filename = sql_file.name
            prefix = filename.split("_")[0]
            try:
                version = int(prefix)
            except ValueError:
                raise ValueError(f"Invalid migration filename format: '{filename}'. Expected '0001_name.sql'")

            content = sql_file.read_text(encoding="utf-8")
            checksum = calculate_migration_checksum(content)

            if version in applied:
                existing = applied[version]
                if existing["checksum"] != checksum:
                    raise ValueError(
                        f"Migration tampering detected! Version {version} ('{filename}') was previously applied "
                        f"with checksum {existing['checksum']}, but file checksum is {checksum}."
                    )
                logger.debug("Migration %s already applied.", filename)
                continue

            logger.info("Applying migration %s...", filename)
            # Apply transactionally
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(content)
                    cur.execute(
                        """
                        INSERT INTO raw.schema_migrations (version, name, checksum, applied_at)
                        VALUES (%s, %s, %s, CLOCK_TIMESTAMP());
                        """,
                        (version, filename, checksum),
                    )
            applied_count += 1
            logger.info("Successfully applied migration %s.", filename)

    return applied_count
