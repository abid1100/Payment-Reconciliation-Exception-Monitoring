-- ====================================================================
-- Migration: 0003_record_dispositions.sql
-- Description: Decouple run-specific dispositions from immutable raw evidence
-- ====================================================================

-- 1. Create Attempt-Specific Dispositions Table
CREATE TABLE IF NOT EXISTS raw.record_dispositions (
    disposition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES raw.pipeline_runs(run_id),
    raw_record_id BIGINT NOT NULL REFERENCES raw.raw_records(record_id),
    disposition VARCHAR(16) NOT NULL CHECK (disposition IN ('ACCEPTED', 'DUPLICATE', 'CONFLICT', 'REJECTED')),
    rejection_reason TEXT,
    canonical_id VARCHAR(64),
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    CONSTRAINT uq_run_record_disposition UNIQUE (run_id, raw_record_id)
);

CREATE INDEX IF NOT EXISTS idx_record_dispositions_run ON raw.record_dispositions(run_id);
CREATE INDEX IF NOT EXISTS idx_record_dispositions_record ON raw.record_dispositions(raw_record_id);

-- 2. Lifecycle Trigger: Dispositions cannot be appended to completed runs
CREATE OR REPLACE FUNCTION raw.guard_disposition_run() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE run_state text;
BEGIN
    SELECT status INTO run_state FROM raw.pipeline_runs
    WHERE run_id = NEW.run_id FOR SHARE;
    IF run_state IS DISTINCT FROM 'RUNNING' THEN
        RAISE EXCEPTION 'New dispositions require an active RUNNING run';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER guard_disposition_run BEFORE INSERT ON raw.record_dispositions
FOR EACH ROW EXECUTE FUNCTION raw.guard_disposition_run();
REVOKE ALL ON FUNCTION raw.guard_disposition_run() FROM PUBLIC;

-- 3. Explicit Role Grants
DO $$
DECLARE
    pipe_role TEXT;
    dash_role TEXT;
BEGIN
    FOREACH pipe_role IN ARRAY ARRAY['recon_pipeline', 'recon_test_pipeline'] LOOP
        IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = pipe_role) THEN
            EXECUTE format('GRANT SELECT, INSERT ON raw.record_dispositions TO %I', pipe_role);
            EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw TO %I', pipe_role);
        END IF;
    END LOOP;

    FOREACH dash_role IN ARRAY ARRAY['recon_dashboard', 'recon_test_dashboard'] LOOP
        IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = dash_role) THEN
            EXECUTE format('GRANT SELECT ON raw.record_dispositions TO %I', dash_role);
        END IF;
    END LOOP;
END $$;
