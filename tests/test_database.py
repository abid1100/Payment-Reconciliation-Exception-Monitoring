"""Tests for database migration checksums, configuration, and security checks."""
import os
import unittest
from pathlib import Path

from reconciliation.config import AppConfig
from reconciliation.migrate import calculate_migration_checksum, get_migrations_dir


class TestDatabaseConfiguration(unittest.TestCase):
    def test_dsn_requires_explicit_env(self):
        config_path = Path(__file__).resolve().parent.parent / "config" / "demo.toml"
        config = AppConfig.load(config_path)

        # Clear env vars if set
        os.environ.pop("RECON_MIGRATION_DSN", None)
        with self.assertRaises(ValueError) as ctx:
            config.get_dsn("migration", is_test=False)
        self.assertIn("RECON_MIGRATION_DSN", str(ctx.exception))

    def test_test_target_guard(self):
        config_path = Path(__file__).resolve().parent.parent / "config" / "demo.toml"
        config = AppConfig.load(config_path)

        os.environ.pop("RECON_TEST_PIPELINE_DSN", None)
        with self.assertRaises(ValueError) as ctx:
            config.get_dsn("pipeline", is_test=True)
        self.assertIn("RECON_TEST_PIPELINE_DSN", str(ctx.exception))


class TestMigrationIntegrity(unittest.TestCase):
    def test_migration_files_exist_and_checksum(self):
        mig_dir = get_migrations_dir()
        files = list(mig_dir.glob("*.sql"))
        self.assertGreaterEqual(len(files), 1, "At least 0001_initial.sql must exist")

        for f in files:
            content = f.read_text(encoding="utf-8")
            checksum = calculate_migration_checksum(content)
            self.assertEqual(len(checksum), 64)
            # Must contain the three mandatory schemas
            self.assertIn("CREATE SCHEMA IF NOT EXISTS raw", content)
            self.assertIn("CREATE SCHEMA IF NOT EXISTS core", content)
            self.assertIn("CREATE SCHEMA IF NOT EXISTS reporting", content)

    def test_migration_contains_role_guards(self):
        mig_dir = get_migrations_dir()
        init_sql = (mig_dir / "0001_initial.sql").read_text(encoding="utf-8")
        self.assertIn("recon_pipeline", init_sql)
        self.assertIn("recon_dashboard", init_sql)
        self.assertIn("REVOKE ALL ON SCHEMA raw, core FROM %I", init_sql)
        self.assertIn("GRANT SELECT, INSERT ON core.orders", init_sql)
        self.assertIn("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON raw.schema_migrations", init_sql)


if __name__ == "__main__":
    unittest.main()
