"""Command-line interface for reconciliation pipeline."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from reconciliation.config import AppConfig
from reconciliation.generate import generate_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reconciliation",
        description="Payment Reconciliation & Exception Monitoring Pipeline CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. generate
    gen_parser = subparsers.add_parser("generate", help="Generate synthetic reproducible batch")
    gen_parser.add_argument("--scenario", default="baseline", help="Scenario name (e.g. baseline, sample_baseline_with_edge_cases)")
    gen_parser.add_argument("--seed", type=int, default=42, help="Provenance seed label (baseline fixture is fixed)")
    gen_parser.add_argument("--base-time", default="2026-01-01T00:00:00Z", help="Base timestamp (ISO-8601 UTC)")
    gen_parser.add_argument("--output", required=True, help="Output directory path")
    gen_parser.add_argument("--batch-id", default=None, help="Explicit batch ID")
    gen_parser.add_argument("--overwrite", action="store_true", help="Explicitly replace generated files in nonempty output")

    # 2. validate
    val_parser = subparsers.add_parser("validate", help="Validate batch files against contracts and manifest")
    val_parser.add_argument("--input", required=True, help="Directory containing batch files and manifest.json")
    val_parser.add_argument("--as-of", default=None, help="Optional timezone-aware evaluation cutoff")

    # 3. migrate
    mig_parser = subparsers.add_parser("migrate", help="Apply versioned schema migrations")
    mig_parser.add_argument("--config", default="config/demo.toml", help="Path to TOML config")
    mig_parser.add_argument("--connection-env", default=None, help="Override environment variable name for DSN")
    mig_parser.add_argument("--is-test", action="store_true", help="Target test database")

    # 4. run
    run_parser = subparsers.add_parser("run", help="Ingest batch, reconcile, and publish snapshot")
    run_parser.add_argument("--input", required=True, help="Directory containing batch files and manifest.json")
    run_parser.add_argument("--as-of", required=True, help="Evaluation timestamp (ISO-8601 UTC)")
    run_parser.add_argument("--config", default="config/demo.toml", help="Path to TOML config")
    run_parser.add_argument("--connection-env", default=None, help="Override environment variable name for DSN")
    run_parser.add_argument("--is-test", action="store_true", help="Target test database")

    # 5. evaluate
    eval_parser = subparsers.add_parser("evaluate", help="Advance evaluation time and re-evaluate pending rules")
    eval_parser.add_argument("--as-of", required=True, help="Evaluation timestamp (ISO-8601 UTC)")
    eval_parser.add_argument("--config", default="config/demo.toml", help="Path to TOML config")
    eval_parser.add_argument("--connection-env", default=None, help="Override environment variable name for DSN")
    eval_parser.add_argument("--is-test", action="store_true", help="Target test database")

    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        return _dispatch(argv)
    except (ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _dispatch(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "generate":
        print(f"Generating synthetic batch (scenario='{args.scenario}', seed={args.seed}) -> {args.output}...")
        manifest = generate_batch(
            output_dir=args.output,
            scenario=args.scenario,
            seed=args.seed,
            base_time=args.base_time,
            batch_id=args.batch_id,
            overwrite=args.overwrite,
        )
        print(f"Success! Generated batch '{manifest['batch_id']}' with {len(manifest['files'])} files.")
        return 0

    if args.command == "validate":
        from reconciliation.batch import prepare_batch
        print(f"Validating batch in {args.input}...")
        prepared = prepare_batch(args.input, as_of=args.as_of)
        errors = [f"{file.filename} line {row.line_number}: {error}"
                  for file in prepared.files for row in file.rows for error in row.errors]
        if errors:
            print("Validation FAILED with the following errors:")
            for err in errors:
                print(f"  - {err}")
            return 1
        print("Validation PASSED! All manifest hashes and record structures match contracts.")
        return 0

    if args.command == "migrate":
        from reconciliation.migrate import run_migrations
        config = AppConfig.load(args.config)
        dsn = config.get_dsn("migration", is_test=args.is_test, connection_env=args.connection_env)
        print("Applying migrations...")
        applied = run_migrations(dsn, lock_id=config.policy.application_lock_id)
        print(f"Applied {applied} migrations successfully.")
        return 0

    if args.command == "run":
        from reconciliation.pipeline import run_pipeline_batch
        config = AppConfig.load(args.config)
        dsn = config.get_dsn("pipeline", is_test=args.is_test, connection_env=args.connection_env)
        print(f"Running pipeline batch from {args.input} as of {args.as_of}...")
        result = run_pipeline_batch(
            input_dir=args.input,
            as_of_time=args.as_of,
            config=config,
            dsn=dsn,
            is_test=args.is_test,
        )
        print(f"Pipeline finished with status: {result['status']}")
        return 0 if result["status"] == "SUCCEEDED" else 1

    if args.command == "evaluate":
        from reconciliation.pipeline import evaluate_existing_state
        config = AppConfig.load(args.config)
        dsn = config.get_dsn("pipeline", is_test=args.is_test, connection_env=args.connection_env)
        print(f"Re-evaluating reconciliation as of {args.as_of}...")
        result = evaluate_existing_state(
            as_of_time=args.as_of,
            config=config,
            dsn=dsn,
            is_test=args.is_test,
        )
        print(f"Evaluation finished with status: {result['status']}")
        return 0 if result["status"] == "SUCCEEDED" else 1

    return 0
