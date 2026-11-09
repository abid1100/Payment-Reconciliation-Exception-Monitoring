-- Upgrade the original schema without editing its recorded migration checksum.
-- Existing data violating the new uniqueness rules causes a safe rollback;
-- it is never silently deleted or merged by this migration.

-- Drop only the two views that depend on the result column types before altering them.
DROP VIEW reporting.v_exception_queue, reporting.v_reconciliation_overview;
ALTER TABLE reporting.reconciliation_results
    ALTER COLUMN reason_code TYPE TEXT,
    ALTER COLUMN difference_minor TYPE NUMERIC;
ALTER TABLE reporting.reconciliation_results ADD CONSTRAINT result_integer_difference
    CHECK (difference_minor IS NULL OR (
        difference_minor NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)
        AND difference_minor = trunc(difference_minor)));
CREATE UNIQUE INDEX reconciliation_result_grain
    ON reporting.reconciliation_results(run_id, rule_id, subject_type, subject_id);

-- Publication guards complement explicit grants; they are not tamper-proof
-- against the database owner, who can deliberately alter schema/permissions.
CREATE FUNCTION raw.guard_run_lifecycle() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'RUNNING' OR NEW.finished_at IS NOT NULL THEN
            RAISE EXCEPTION 'A new run must start as RUNNING';
        END IF;
    ELSE
        IF OLD.status <> 'RUNNING' THEN
            RAISE EXCEPTION 'A terminal run cannot be modified';
        END IF;
        IF (NEW.run_id, NEW.run_sequence, NEW.as_of, NEW.config_version,
            NEW.rule_version, NEW.started_at) IS DISTINCT FROM
           (OLD.run_id, OLD.run_sequence, OLD.as_of, OLD.config_version,
            OLD.rule_version, OLD.started_at) THEN
            RAISE EXCEPTION 'Run identity and evaluation policy are immutable';
        END IF;
        IF NEW.status <> 'RUNNING' AND NEW.finished_at IS NULL THEN
            RAISE EXCEPTION 'A terminal run requires finished_at';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_run_lifecycle BEFORE INSERT OR UPDATE ON raw.pipeline_runs
FOR EACH ROW EXECUTE FUNCTION raw.guard_run_lifecycle();

CREATE FUNCTION raw.guard_running_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE run_state text;
BEGIN
    IF TG_TABLE_SCHEMA = 'reporting' THEN
        SELECT status INTO run_state FROM raw.pipeline_runs
        WHERE run_id = NEW.run_id FOR SHARE;
    ELSE
        SELECT status INTO run_state FROM raw.pipeline_runs
        WHERE run_sequence = NEW.loaded_run_sequence FOR SHARE;
    END IF;
    IF run_state IS DISTINCT FROM 'RUNNING' THEN
        RAISE EXCEPTION 'New facts/results require an active RUNNING run';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_result_run BEFORE INSERT ON reporting.reconciliation_results
FOR EACH ROW EXECUTE FUNCTION raw.guard_running_insert();
DO $$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['orders','payment_events','transactions','settlement_entries','payouts'] LOOP
        EXECUTE format('CREATE TRIGGER guard_fact_run BEFORE INSERT ON core.%I FOR EACH ROW EXECUTE FUNCTION raw.guard_running_insert()', table_name);
    END LOOP;
END $$;
REVOKE ALL ON FUNCTION raw.guard_run_lifecycle(), raw.guard_running_insert() FROM PUBLIC;

-- Counts include a successful empty evaluation; no overlapping cross-rule money KPI.
CREATE VIEW reporting.v_reconciliation_overview AS
SELECT pr.run_id, pr.as_of, pr.status AS run_status,
       count(r.result_id) AS total_rules_evaluated,
       count(r.result_id) FILTER (WHERE r.state = 'MATCHED') AS matched_count,
       count(r.result_id) FILTER (WHERE r.state = 'PENDING') AS pending_count,
       count(r.result_id) FILTER (WHERE r.state = 'EXCEPTION') AS exception_count
FROM raw.pipeline_runs pr
LEFT JOIN reporting.reconciliation_results r ON r.run_id = pr.run_id
WHERE pr.status = 'SUCCEEDED'
GROUP BY pr.run_id, pr.as_of, pr.status;

CREATE OR REPLACE VIEW reporting.v_exception_queue AS
SELECT r.result_id, r.run_id, r.rule_id, r.subject_type, r.subject_id,
       r.state, r.reason_code, r.expected_value, r.observed_value,
       r.difference_minor, r.due_at, r.evaluated_at, r.evidence
FROM reporting.reconciliation_results r
JOIN raw.pipeline_runs pr ON pr.run_id = r.run_id AND pr.status = 'SUCCEEDED'
WHERE r.state = 'EXCEPTION';

CREATE OR REPLACE VIEW reporting.v_transaction_lineage AS
SELECT t.transaction_id, t.transaction_type,
       COALESCE(t.order_id, parent.order_id) AS order_id, t.parent_payment_id,
       t.amount_minor AS transaction_amount_minor, o.amount_minor AS order_amount_minor,
       se.settlement_entry_id, se.payout_id, se.gross_minor AS settled_gross_minor,
       se.fee_minor AS settled_fee_minor, se.net_minor AS settled_net_minor,
       p.amount_minor AS payout_summary_amount_minor,
       t.occurred_at, se.settled_at, p.issued_at, pr.run_id
FROM raw.pipeline_runs pr
JOIN core.transactions t ON t.loaded_run_sequence <= pr.run_sequence
LEFT JOIN core.transactions parent
    ON parent.transaction_id = t.parent_payment_id
    AND parent.transaction_type = 'payment' AND parent.loaded_run_sequence <= pr.run_sequence
LEFT JOIN core.orders o
    ON o.order_id = COALESCE(t.order_id, parent.order_id) AND o.loaded_run_sequence <= pr.run_sequence
LEFT JOIN core.settlement_entries se
    ON se.transaction_id = t.transaction_id AND se.loaded_run_sequence <= pr.run_sequence
LEFT JOIN core.payouts p
    ON p.payout_id = se.payout_id AND p.loaded_run_sequence <= pr.run_sequence
WHERE pr.status = 'SUCCEEDED';

CREATE VIEW reporting.v_rule_summary AS
SELECT r.run_id, r.rule_id, r.state, count(*) AS result_count,
       sum(r.difference_minor) AS signed_difference_minor
FROM reporting.reconciliation_results r
JOIN raw.pipeline_runs pr ON pr.run_id = r.run_id AND pr.status = 'SUCCEEDED'
GROUP BY r.run_id, r.rule_id, r.state;
COMMENT ON VIEW reporting.v_rule_summary IS
    'Per-rule diagnostic amounts only. Do not sum across rules or label as financial loss.';

CREATE VIEW reporting.v_event_evidence AS
SELECT pr.run_id, e.event_id, e.transaction_id, e.event_type, e.order_id,
       e.parent_payment_id, e.amount_minor, e.currency, e.occurred_at, e.received_at,
       rr.record_id, rr.batch_id, rr.file_hash, rr.line_number, rr.raw_payload
FROM raw.pipeline_runs pr
JOIN core.payment_events e ON e.loaded_run_sequence <= pr.run_sequence
JOIN raw.raw_records rr ON rr.record_id = e.raw_record_id
WHERE pr.status = 'SUCCEEDED';

DO $$
DECLARE owner_role text; pipe_role text; dash_role text; foreign_role text;
BEGIN
    CASE current_database()
        WHEN 'recon_dev' THEN
            owner_role := 'recon_owner'; pipe_role := 'recon_pipeline'; dash_role := 'recon_dashboard';
        WHEN 'recon_test' THEN
            owner_role := 'recon_test_owner'; pipe_role := 'recon_test_pipeline'; dash_role := 'recon_test_dashboard';
        ELSE RAISE EXCEPTION 'Migrations require the explicit recon_dev or recon_test target';
    END CASE;
    IF current_user <> owner_role THEN
        RAISE EXCEPTION 'Run migrations as the designated environment owner';
    END IF;
    -- Remove grants made to the other environment by the original migration.
    FOREACH foreign_role IN ARRAY ARRAY['recon_pipeline','recon_dashboard','recon_test_pipeline','recon_test_dashboard'] LOOP
        IF foreign_role NOT IN (pipe_role, dash_role)
           AND EXISTS (SELECT FROM pg_roles WHERE rolname = foreign_role) THEN
            EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA raw, core, reporting FROM %I', foreign_role);
            EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA raw, core, reporting FROM %I', foreign_role);
            EXECUTE format('REVOKE ALL ON SCHEMA raw, core, reporting FROM %I', foreign_role);
        END IF;
    END LOOP;
    EXECUTE format('GRANT EXECUTE ON FUNCTION raw.guard_run_lifecycle(), raw.guard_running_insert() TO %I', pipe_role);
    REVOKE CREATE ON SCHEMA public FROM PUBLIC;
    EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format('GRANT SELECT ON reporting.v_reconciliation_overview, reporting.v_exception_queue, reporting.v_transaction_lineage, reporting.v_pipeline_health, reporting.v_rule_summary, reporting.v_event_evidence TO %I', dash_role);
END $$;
