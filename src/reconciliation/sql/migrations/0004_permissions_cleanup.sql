-- ====================================================================
-- Migration: 0004_permissions_cleanup.sql
-- Description: Restore strict environment boundaries and least-privilege grants
--              and project raw-dependent conflict evidence behind reporting view
-- ====================================================================

-- 1. Create approved reporting view for unresolved source conflicts
-- This view runs with owner privileges, projecting conflict evidence without
-- requiring direct SELECT on raw.record_dispositions or raw.raw_records for dashboards.
CREATE OR REPLACE VIEW reporting.v_unresolved_conflicts AS
SELECT
    d.disposition_id,
    d.run_id AS detected_run_id,
    pr.run_sequence AS detected_run_sequence,
    r.source_type,
    d.canonical_id AS subject_id,
    d.rejection_reason,
    r.record_id,
    d.evaluated_at
FROM raw.record_dispositions d
JOIN raw.raw_records r ON r.record_id = d.raw_record_id
JOIN raw.pipeline_runs pr ON pr.run_id = d.run_id
WHERE pr.status = 'SUCCEEDED'
  AND d.disposition = 'CONFLICT';

COMMENT ON VIEW reporting.v_unresolved_conflicts IS
    'Projects unresolved source conflicts from completed runs with immutable detection provenance for operational queue consumers.';

DO $$
DECLARE
    owner_role text;
    pipe_role text;
    dash_role text;
    foreign_role text;
    dash_role_name text;
BEGIN
    -- 2. Identify active environment and require execution by environment owner
    CASE current_database()
        WHEN 'recon_dev' THEN
            owner_role := 'recon_owner';
            pipe_role := 'recon_pipeline';
            dash_role := 'recon_dashboard';
        WHEN 'recon_test' THEN
            owner_role := 'recon_test_owner';
            pipe_role := 'recon_test_pipeline';
            dash_role := 'recon_test_dashboard';
        ELSE
            RAISE EXCEPTION 'Migrations require the explicit recon_dev or recon_test target database';
    END CASE;

    IF current_user <> owner_role THEN
        RAISE EXCEPTION 'Run migrations as the designated environment owner (%)', owner_role;
    END IF;

    -- 3. Revoke dashboard roles from raw.record_dispositions (dashboards read reporting views only)
    FOREACH dash_role_name IN ARRAY ARRAY['recon_dashboard', 'recon_test_dashboard'] LOOP
        IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = dash_role_name) THEN
            EXECUTE format('REVOKE ALL ON raw.record_dispositions FROM %I', dash_role_name);
        END IF;
    END LOOP;

    -- 4. Grant reporting.v_unresolved_conflicts to active dashboard and pipeline roles
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = dash_role) THEN
        EXECUTE format('GRANT SELECT ON reporting.v_unresolved_conflicts TO %I', dash_role);
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = pipe_role) THEN
        EXECUTE format('GRANT SELECT ON reporting.v_unresolved_conflicts TO %I', pipe_role);
    END IF;

    -- 5. Revoke grants from foreign / wrong-environment pipeline and dashboard roles
    FOREACH foreign_role IN ARRAY ARRAY['recon_pipeline', 'recon_dashboard', 'recon_test_pipeline', 'recon_test_dashboard'] LOOP
        IF foreign_role NOT IN (pipe_role, dash_role) AND EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = foreign_role) THEN
            EXECUTE format('REVOKE ALL ON raw.record_dispositions FROM %I', foreign_role);
            EXECUTE format('REVOKE ALL ON reporting.v_unresolved_conflicts FROM %I', foreign_role);
            EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA raw FROM %I', foreign_role);
            EXECUTE format('REVOKE USAGE ON SCHEMA raw, core, reporting FROM %I', foreign_role);
        END IF;
    END LOOP;

    -- 6. Scope active pipeline role permissions strictly
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = pipe_role) THEN
        -- Revoke blanket sequence grant introduced in 0003
        EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA raw FROM %I', pipe_role);
        -- Re-grant specific sequence usage on identity sequence only
        EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE raw.record_dispositions_disposition_id_seq TO %I', pipe_role);
        -- Maintain SELECT, INSERT on the table itself
        EXECUTE format('GRANT SELECT, INSERT ON raw.record_dispositions TO %I', pipe_role);
    END IF;
END $$;
