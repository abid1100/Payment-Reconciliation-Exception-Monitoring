"""Configuration loader and validator for reconciliation pipeline."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


def _integer(value: Any, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _env_name(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Connection environment variable name is invalid")
    return value


def _check_dsn(dsn: str, role: str, is_test: bool) -> None:
    # Import only on database commands: file validation/generation need no driver.
    from psycopg.conninfo import conninfo_to_dict
    try:
        params = conninfo_to_dict(dsn)
        if dsn.startswith(("postgresql://", "postgres://")):
            uri = urlsplit(dsn)
            keys = [key for key, _ in parse_qsl(uri.query, keep_blank_values=True)]
            if uri.path and uri.path != "/":
                keys.append("dbname")
            if uri.username is not None:
                keys.append("user")
            if uri.hostname:
                keys.append("host")
            if uri.port is not None:
                keys.append("port")
        else:
            # libpq allows escaped and single-quoted keyword values. Scan only
            # actual assignments, never tokens embedded inside a password.
            assignment = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:'(?:\\.|[^'\\])*'|(?:\\.|[^\s'\\])*)")
            keys = []
            position = 0
            while position < len(dsn.rstrip()):
                match = assignment.match(dsn, position)
                if match is None:
                    raise ValueError("Invalid connection assignments")
                keys.append(match.group(1))
                position = match.end()
        if len(keys) != len(set(keys)) or "service" in params:
            raise ValueError("Ambiguous connection parameters")
        if any("," in params.get(key, "") for key in ("host", "hostaddr", "port")):
            raise ValueError("Multiple connection targets are not allowed")
        database = "recon_test" if is_test else "recon_dev"
        prefix = "recon_test" if is_test else "recon"
        user = prefix + "_" + {"migration": "owner", "pipeline": "pipeline", "dashboard": "dashboard"}[role]
        if params.get("dbname") != database or params.get("user") != user:
            raise ValueError("Connection database/user does not match requested role")
    except Exception:
        # Parser errors may embed the full DSN, including passwords. Suppress
        # the original message AND exception chain at this trust boundary.
        raise ValueError("Invalid or unsafe database connection for requested role/environment") from None


@dataclass(frozen=True)
class ReconciliationPolicy:
    currency: str
    grace_period_hours: int
    rule_version: str
    config_version: str
    schema_version: str
    tolerance_minor: int
    batch_size: int
    log_level: str
    application_lock_id: int

    def to_snapshot_dict(self) -> dict[str, Any]:
        """Returns non-secret policy configuration safe for snapshotting in audit logs."""
        return {
            "currency": self.currency,
            "grace_period_hours": self.grace_period_hours,
            "rule_version": self.rule_version,
            "config_version": self.config_version,
            "schema_version": self.schema_version,
            "tolerance_minor": self.tolerance_minor,
            "batch_size": self.batch_size,
        }


@dataclass(frozen=True)
class AppConfig:
    policy: ReconciliationPolicy
    migration_dsn_env: str
    pipeline_dsn_env: str
    dashboard_dsn_env: str
    test_migration_dsn_env: str
    test_pipeline_dsn_env: str
    test_dashboard_dsn_env: str

    @classmethod
    def load(cls, config_path: str | Path) -> AppConfig:
        path = Path(config_path)
        if not path.is_file():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, "rb") as f:
            data = tomllib.load(f)

        recon_data = data.get("reconciliation", {})
        pipe_data = data.get("pipeline", {})
        db_data = data.get("database", {})

        currency = recon_data.get("currency", "INR")
        if currency != "INR":
            raise ValueError(f"Unsupported currency: {currency}. Version 1.0 requires INR.")

        # One year is a deliberately conservative v1 policy ceiling.
        grace_hours = _integer(recon_data.get("grace_period_hours", 72), "grace_period_hours", 1, 8760)
        schema_version = recon_data.get("schema_version", "1.0.0")
        if schema_version != "1.0.0":
            raise ValueError("Only schema_version 1.0.0 is supported")

        policy = ReconciliationPolicy(
            currency=currency,
            grace_period_hours=grace_hours,
            rule_version=str(recon_data.get("rule_version", "1.0.0")),
            config_version=str(recon_data.get("config_version", "1.0.0")),
            schema_version=schema_version,
            tolerance_minor=_integer(recon_data.get("tolerance_minor", 0), "tolerance_minor", 0, 0),
            batch_size=_integer(pipe_data.get("batch_size", 1000), "batch_size", 1, 2**31 - 1),
            log_level=str(pipe_data.get("log_level", "INFO")),
            application_lock_id=_integer(pipe_data.get("application_lock_id", 428910001), "application_lock_id", -(2**63), 2**63 - 1),
        )

        return cls(
            policy=policy,
            migration_dsn_env=_env_name(db_data.get("migration_dsn_env", "RECON_MIGRATION_DSN")),
            pipeline_dsn_env=_env_name(db_data.get("pipeline_dsn_env", "RECON_PIPELINE_DSN")),
            dashboard_dsn_env=_env_name(db_data.get("dashboard_dsn_env", "RECON_DASHBOARD_DSN")),
            test_migration_dsn_env=_env_name(db_data.get("test_migration_dsn_env", "RECON_TEST_MIGRATION_DSN")),
            test_pipeline_dsn_env=_env_name(db_data.get("test_pipeline_dsn_env", "RECON_TEST_PIPELINE_DSN")),
            test_dashboard_dsn_env=_env_name(db_data.get("test_dashboard_dsn_env", "RECON_TEST_DASHBOARD_DSN")),
        )

    def get_dsn(self, role: str, is_test: bool = False, connection_env: str | None = None) -> str:
        """Resolve database DSN from explicitly named environment variables without fallback."""
        env_map = {
            "migration": self.test_migration_dsn_env if is_test else self.migration_dsn_env,
            "pipeline": self.test_pipeline_dsn_env if is_test else self.pipeline_dsn_env,
            "dashboard": self.test_dashboard_dsn_env if is_test else self.dashboard_dsn_env,
        }
        env_name = env_map.get(role)
        if not env_name:
            raise ValueError(f"Unknown database role: {role}. Expected 'migration', 'pipeline', or 'dashboard'.")
        env_name = _env_name(connection_env if connection_env is not None else env_name)

        dsn = os.getenv(env_name)
        if not dsn:
            raise ValueError(
                f"Required database connection environment variable '{env_name}' is not set. "
                f"Provide DSN explicitly or consult .env.example."
            )
        _check_dsn(dsn, role, is_test)
        return dsn
