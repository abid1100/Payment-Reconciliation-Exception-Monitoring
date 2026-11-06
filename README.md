# Payment Reconciliation & Exception Monitoring

**A local-first data engineering portfolio project for matching orders, payment events, refunds and payout records—and explaining the differences.**

> **Project status: planning / documentation only.** This README describes the intended system. The application, sample generator, dashboard and automated tests have not been implemented or verified as part of this documentation package. There is no runnable release or hosted demo yet.
>
> **Data policy:** the first release will use clearly labelled synthetic data. No real money will be moved, and no real customer or banking data is required.

## At a glance

| Item | Proposed scope |
| --- | --- |
| Intended users | A simulated finance-operations analyst and a data engineer |
| Business question | Can each expected payment, refund and payout be explained by its supporting records? |
| Core output | Reconciliation results, an exception queue and traceable source evidence |
| Engineering focus | SQL modelling, data validation, idempotent loading, transactional recovery and operational visibility |
| Stack | PostgreSQL (user-selected database); Python, SQL, pytest and a small Streamlit interface remain the supporting proposal |
| Processing model | Local batch processing; not a live payment-processing service |
| Initial currency | INR only, with amounts represented in integer minor units |
| Current deliverable | A project specification and repository handoff, not completed software |

## Contents

- The business problem
- Intended workflow and architecture
- Data sources and model
- Reconciliation rules
- Reliability and recovery
- Proposed dashboard
- Repository structure
- Running the project
- Testing and verification
- Implementation roadmap
- Scope, limitations and provenance

## The business problem

An order record, a payment event and a payout record describe different parts of a transaction. A useful reconciliation system must establish whether those records agree, whether a difference is still expected because of timing, and whether a person needs to investigate.

The proposed project asks:

- Does an order have the expected captured payment?
- Does the captured amount agree with the order amount?
- Is a refund linked to the correct payment?
- Has a payment or refund appeared in a settlement batch?
- Do settlement-line totals agree with the payout summary?
- Is a missing record still within the demonstration waiting window, or overdue?
- Can every result be traced to the records and processing run that produced it?

A payment-provider payout report is **not** independent proof that a bank received the money. Bank-statement reconciliation is outside the initial scope.

## Intended workflow and architecture

```text
Synthetic order export       Synthetic payment-event export
        CSV                              JSONL
          \                                /
           \                              /
             Preserve inputs and register batches
                           |
               Parse, validate and deduplicate
                           |
             PostgreSQL canonical source tables
                           |
Synthetic settlement CSVs -> Match records and evaluate rules
                           |
              Versioned reconciliation results
                           |
           SQL summaries and read-only dashboard
                           |
       Overview | Exceptions | Evidence | Pipeline health
```

Every stage above is a **planned component**, not a claim of current implementation.

The initial design uses a local PostgreSQL server and batch inputs. PostgreSQL is the selected database for the pipeline, dashboard and database integration tests; a parallel database implementation is not in scope. CSV/JSON inputs and separate development/test databases remain supported design requirements. A distributed stack, cloud deployment and live webhooks are not prerequisites.

Installation method, PostgreSQL version and Python driver must be discussed before implementation; none has been installed or selected by this documentation update. Major architecture or scope changes require discussion with the project owner first.

## Data sources

The demonstration will represent three source systems using four input files:

| Source system | File | Intended record grain |
| --- | --- | --- |
| Order system | `orders.csv` | One completed checkout expecting one successful captured payment |
| Payment provider | `payment_events.jsonl` | One delivered event describing a successful payment or refund |
| Settlement reporting | `settlement_entries.csv` | One settlement line for a payment or refund |
| Settlement reporting | `payouts.csv` | One payout-summary record |

A separate `manifest.json` will record the batch ID, schema version, generation seed, fixed base time and file hashes. The tiny committed sample will have a provenance note identifying it as synthetic.

### Data conventions

- Store currency amounts as integers in minor units; never use binary floating-point equality for reconciliation.
- Keep the initial dataset in INR only.
- Use timezone-aware UTC timestamps and an explicit evaluation time.
- Preserve source identifiers, file hashes and row/line locations.
- Keep event delivery identity separate from the identity of the underlying payment or refund.
- Treat source files as immutable evidence rather than overwriting them during cleaning.
- Keep scenario labels and expected test answers separate from application inputs.

The detailed proposed contracts belong in `PROJECT_HANDOFF.md`; a maintained `docs/data-dictionary.md` is part of the implementation plan.

## Proposed data model

The planned database separates evidence, canonical records and derived results:

| Layer | Planned tables | Purpose |
| --- | --- | --- |
| Evidence and operations (`raw`) | `pipeline_runs`, `source_batches`, `raw_records`, `schema_migrations` | Track processing, source identity, row-level outcomes and applied migrations |
| Canonical business data (`core`) | `orders`, `payment_events`, `transactions`, `settlement_entries`, `payouts` | Store validated, deduplicated source facts |
| Reconciliation (`reporting`) | `reconciliation_results` and explicit reporting views | Store run-specific rule outcomes; expose reviewed read-only outputs |

A payment and a refund will be separate business transactions. Multiple delivered events must not cause the same business transaction to be counted repeatedly.

Derived results will be versioned by run. The dashboard should resolve the latest successfully committed run once per render, use that same run ID for every business-data query and display its evaluation time.

The planned access model separates a migration/owner role, a limited pipeline writer and a dashboard reader. The reader receives access only to approved reporting views, including evidence and health views—not direct raw tables. Database names and roles for tests must be isolated from the development dataset. These permissions are acceptance criteria, not implemented controls.

## Reconciliation rules

These are **project-specific demonstration rules**, not a representation of a payment provider's full accounting policy.

| Check | Intended behaviour |
| --- | --- |
| Order to payment | Match by explicit identifiers; compare amount and currency |
| Refund to payment | Require a valid parent payment and check cumulative refunds against the captured amount |
| Transaction to settlement | Match each payment/refund to its settlement line and compare signed amounts |
| Settlement arithmetic | Verify that signed gross amount minus fee equals signed net amount |
| Settlement to payout | Compare the sum of settlement-line net amounts with the payout-summary amount |
| Missing dependency | Keep structurally valid records available for later matching; distinguish waiting from overdue |
| Duplicate delivery | Record delivery evidence without double-counting the event or transaction |
| Conflicting identifier | Flag the conflict; do not silently overwrite the earlier canonical record |

### Result states

- **MATCHED:** the applicable check agrees.
- **PENDING:** a required counterpart has not arrived, but the configured waiting window has not expired.
- **EXCEPTION:** a mismatch, conflict or overdue missing counterpart needs investigation.

Invalid input rows are recorded separately as rejected data; they are not quietly converted into matched transactions or zero amounts.

The initial missing-counterpart window is proposed as **72 hours**, configurable and measured against an explicit evaluation time. It is a simulation setting—not a claim about real settlement schedules. An older pending result must be reconsidered even if no new files arrive.

## Reliability and recovery goals

The implementation should demonstrate:

1. **Idempotent loading:** retrying the same input must not create duplicate business records.
2. **Deterministic evaluation:** identical source state, configuration, rules and evaluation time should produce identical business results, excluding operational run metadata.
3. **Late-record recovery:** a delayed settlement or parent payment should resolve the appropriate pending result without dropping its history.
4. **Controlled rejection:** malformed rows should retain their source location and a useful reason.
5. **Transactional publication:** a failed run must not become the dashboard's latest successful result.
6. **Replayability:** preserved inputs should support rebuilding results in a fresh database.
7. **Observable processing:** each run should report its inputs, outcomes, duration and failure state.
8. **Database operations:** verify restricted roles, versioned schema migrations and a PostgreSQL backup restored into a separate database. Replaying inputs and restoring a backup are separate checks.

These are acceptance criteria. None is marked achieved until corresponding tests and execution evidence exist.

## Proposed dashboard

### 1. Reconciliation overview

Show the selected run, evaluation time, matched/pending/exception counts and currency-labelled totals. Distinguish transaction-level measures from payout-level measures rather than adding overlapping totals together.

### 2. Exception queue

Filter by rule, record type and age. Show the expected value, observed value, difference, first-seen run and current status. Do not label an entire payout amount as a confirmed loss.

### 3. Transaction evidence

Show the order, payment/refund events and settlement references for one transaction. Include source file and row/line provenance, not just a status badge.

### 4. Pipeline health

Show the latest successful run, failed/interrupted attempts, input coverage, rejected records and duplicate/conflict counts. Missing data should be shown as missing—not as a fabricated zero.

**Screenshots and a recorded demo are not available yet.** They should be added only after the underlying application works.

## Repository structure

The following is the **target structure**. Most entries do not exist in this documentation-only package.

```text
payment-reconciliation/
|-- README.md
|-- PROJECT_HANDOFF.md
|-- .gitignore
|-- .env.example
|-- pyproject.toml
|-- requirements-lock.txt
|-- config/
|   `-- demo.toml
|-- src/
|   `-- reconciliation/
|       |-- __init__.py
|       |-- __main__.py
|       |-- cli.py
|       |-- config.py
|       |-- generate.py
|       |-- contracts.py
|       |-- ingest.py
|       |-- database.py
|       |-- migrate.py
|       |-- reconcile.py
|       |-- pipeline.py
|       |-- queries.py
|       `-- sql/
|           |-- reconciliation.sql
|           |-- reporting.sql
|           `-- migrations/
|               `-- 0001_initial.sql
|-- app/
|   `-- dashboard.py
|-- tests/
|   |-- conftest.py
|   |-- test_generation.py
|   |-- test_validation.py
|   |-- test_ingestion.py
|   |-- test_database.py
|   |-- test_reconciliation.py
|   |-- test_late_arrivals.py
|   |-- test_recovery.py
|   |-- test_reporting.py
|   `-- fixtures/
|       `-- README.md
|-- data/
|   `-- sample/
|       |-- README.md
|       |-- manifest.json
|       |-- orders.csv
|       |-- payment_events.jsonl
|       |-- settlement_entries.csv
|       `-- payouts.csv
|-- docs/
|   |-- architecture.md
|   |-- data-dictionary.md
|   |-- reconciliation-rules.md
|   |-- decisions.md
|   |-- runbook.md
|   |-- demo-script.md
|   `-- assets/
|-- reports/
|   `-- verification.md
`-- .github/
    `-- workflows/
        `-- tests.yml
```

SQL files sit inside the Python package so the eventual installation can include them as package resources. The handoff specifies what each file is responsible for and which generated files must stay out of Git.

## Running the project

**There is no runnable application in this documentation-only version.** Installing packages or following guessed commands will not make the missing implementation available.

The intended development baseline is Python 3.11 or later, subject to actual dependency and platform testing. PostgreSQL is the selected database; pytest is the proposed test runner; Streamlit is the proposed presentation layer. A PostgreSQL server, a compatible Python driver and separate role-specific connections will be required. Server/driver versions and installation method are still to be agreed and tested.

The planned `.env.example` will list blank connection-setting names only; it is not delivered yet. Runtime code will read explicitly supplied process settings or an agreed local credential mechanism. Merely creating an `.env` file will not load it automatically. Real credentials must never appear in TOML, command examples, logs or Git. Development, migration, dashboard and test connections must not silently fall back to one shared privileged account.

When a working release exists, this section must contain the exact commands successfully tested from a clean checkout for:

1. Creating an isolated environment.
2. Installing the project, locked dependencies and the agreed local PostgreSQL setup.
3. Provisioning the dedicated databases and roles, configuring connections, then applying versioned migrations and grants.
4. Generating or selecting the synthetic sample.
5. Running a deterministic reconciliation batch.
6. Running automated integration tests against a dedicated PostgreSQL test database.
7. Opening the dashboard with its read-only account.
8. Reproducing failure recovery and restoring a backup into a separate disposable database.

The proposed command interface is documented in the handoff as an implementation target, not as working installation instructions.

## Testing and verification

| Scenario | Required observation | Current evidence |
| --- | --- | --- |
| Correct baseline | All intended checks match | Not run |
| Identical input replay | Business counts and totals do not change | Not run |
| Duplicate event with a different delivery ID | One business transaction remains | Not run |
| Conflicting identifier | Conflict is visible; original data is not silently replaced | Not run |
| Late settlement | Pending/overdue result is resolved by the new counterpart | Not run |
| Time advances without new input | Pending item becomes overdue at the defined boundary | Not run |
| Refund arrives before its parent | Record is retained and later matched | Not run |
| Malformed row | Rejection includes an understandable reason and source location | Not run |
| Payout discrepancy | Signed difference is reported without claiming confirmed loss | Not run |
| Mid-run failure | No partial successful result is published | Not run |
| Fresh-database rebuild | Business results agree for the same inputs/configuration/time | Not run |
| Dashboard read failure | Explicit error or no-data state; no invented values | Not run |
| Role restrictions | Dashboard cannot write or read raw tables; pipeline cannot perform DDL | Not run |
| Versioned migration | Fresh install and an upgrade fixture pass; edited applied migrations are refused | Not run |
| Backup restore | A separate restored database agrees on known rows, totals, constraints and grants | Not run |
| Test isolation and CI | PostgreSQL-backed tests cannot target the development database | Not run |

A future `reports/verification.md` should record the commit, environment, commands, fixtures, actual outcomes and remaining limitations. A green badge or performance number should not be added without supporting execution evidence.

## Implementation roadmap

- [ ] Specify source contracts, rules and hand-checkable fixtures.
- [ ] Implement deterministic sample generation and validation.
- [ ] Agree the PostgreSQL installation method/versions; implement connections, roles, versioned migrations, provenance and idempotent ingestion.
- [ ] Implement matching, timing rules and SQL summaries.
- [ ] Verify late arrivals, conflicts, rollback, replay, permissions and backup/restore.
- [ ] Build the read-only dashboard.
- [ ] Verify a clean-checkout workflow and automated CI backed by a dedicated PostgreSQL service.
- [ ] Record the demonstration and publish evidence-backed results.

## Scope and limitations

The initial design is deliberately bounded:

- One synthetic provider, one currency and one successful capture per order.
- Partial refunds are supported in the design; split payments and partial captures are not.
- No chargebacks, FX, reserves, tax accounting, accounting-ledger certification or real bank-statement matching.
- No live webhook server, payment execution, production credentials or automated refunds.
- No mandatory Kafka, Spark, Airflow, Docker or cloud account.
- No claim of production readiness, financial compliance, employment experience or measured financial savings.
- Performance and scale remain unmeasured.

Future extensions should follow a demonstrated need, not simply expand the tool list.

## Data safety, provenance and authorship

Commit source code, tiny synthetic fixtures, reviewed configuration and documentation. Keep credentials, virtual environments, PostgreSQL data directories, backup dumps, logs, generated exports and real financial records out of the repository. Keep plain-SQL backups under an ignored backup directory; do not ignore all `.sql` files because source queries and migrations belong in Git.

Describe personal contributions and AI assistance accurately when implementation begins. Generated code alone is not evidence of independent understanding. This is an independent portfolio project, not a claim of work performed for a real financial organization.

No software license has been selected in this planning package. Select one deliberately before advertising reuse rights.

## Design references

These references informed the problem framing and recovery requirements; they do not establish that this project implements a provider's API or accounting model. Checked on 2026-11-06.

- Stripe: webhook delivery, ordering and duplicate-handling considerations.
- Stripe: payout reconciliation and the distinction between payout summaries and their component transactions.
- PostgreSQL: schemas and privileges, transaction/locking behaviour and logical backup/restore.
- GitHub: repository README purpose and presentation.

```text
https://docs.stripe.com/webhooks
https://docs.stripe.com/reports/payout-reconciliation
https://www.postgresql.org/docs/current/ddl-schemas.html
https://www.postgresql.org/docs/current/ddl-priv.html
https://www.postgresql.org/docs/current/explicit-locking.html
https://www.postgresql.org/docs/current/backup-dump.html
https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes
```

The project contracts and rules above are proposed design choices. They are not copied provider schemas or verified implementation results.

