"""Configuration loader and validator for reconciliation pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


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

        currency = str(recon_data.get("currency", "INR")).upper()
        if currency != "INR":
            raise ValueError(f"Unsupported currency: {currency}. Version 1.0 requires INR.")

        grace_hours = int(recon_data.get("grace_period_hours", 72))
        if grace_hours <= 0:
            raise ValueError("grace_period_hours must be a positive integer.")

        policy = ReconciliationPolicy(
            currency=currency,
            grace_period_hours=grace_hours,
            rule_version=str(recon_data.get("rule_version", "1.0.0")),
            config_version=str(recon_data.get("config_version", "1.0.0")),
            schema_version=str(recon_data.get("schema_version", "1.0.0")),
            tolerance_minor=int(recon_data.get("tolerance_minor", 0)),
            batch_size=int(pipe_data.get("batch_size", 1000)),
            log_level=str(pipe_data.get("log_level", "INFO")),
            application_lock_id=int(pipe_data.get("application_lock_id", 428910001)),
        )

        return cls(
            policy=policy,
            migration_dsn_env=str(db_data.get("migration_dsn_env", "RECON_MIGRATION_DSN")),
            pipeline_dsn_env=str(db_data.get("pipeline_dsn_env", "RECON_PIPELINE_DSN")),
            dashboard_dsn_env=str(db_data.get("dashboard_dsn_env", "RECON_DASHBOARD_DSN")),
            test_migration_dsn_env=str(db_data.get("test_migration_dsn_env", "RECON_TEST_MIGRATION_DSN")),
            test_pipeline_dsn_env=str(db_data.get("test_pipeline_dsn_env", "RECON_TEST_PIPELINE_DSN")),
            test_dashboard_dsn_env=str(db_data.get("test_dashboard_dsn_env", "RECON_TEST_DASHBOARD_DSN")),
        )

    def get_dsn(self, role: str, is_test: bool = False) -> str:
        """Resolve database DSN from explicitly named environment variables without fallback."""
        env_map = {
            "migration": self.test_migration_dsn_env if is_test else self.migration_dsn_env,
            "pipeline": self.test_pipeline_dsn_env if is_test else self.pipeline_dsn_env,
            "dashboard": self.test_dashboard_dsn_env if is_test else self.dashboard_dsn_env,
        }
        env_name = env_map.get(role)
        if not env_name:
            raise ValueError(f"Unknown database role: {role}. Expected 'migration', 'pipeline', or 'dashboard'.")

        dsn = os.getenv(env_name)
        if not dsn:
            raise ValueError(
                f"Required database connection environment variable '{env_name}' is not set. "
                f"Provide DSN explicitly or consult .env.example."
            )
        return dsn
