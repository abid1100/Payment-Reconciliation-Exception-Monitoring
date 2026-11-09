-- ====================================================================
-- Migration: 0005_exact_byte_evidence.sql
-- Description: Durable exact-byte evidence storage and batch delivery provenance
-- ====================================================================

-- Archive completeness begins with this migration.
-- Historical batches prior to migration 0005 have no archive associations and are
-- classified as "original evidence unavailable". No synthetic original bytes are
-- backfilled from parsed/stored text.

-- 1. Content-addressed immutable evidence blob storage
CREATE TABLE IF NOT EXISTS raw.evidence_blobs (
    blob_hash VARCHAR(64) PRIMARY KEY,
    byte_length BIGINT NOT NULL CHECK (byte_length >= 0),
    raw_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    CONSTRAINT chk_blob_length CHECK (octet_length(raw_bytes) = byte_length)
);

COMMENT ON TABLE raw.evidence_blobs IS
    'Content-addressed immutable byte store keyed by SHA-256 for exact source file and manifest payloads.';

-- 2. Batch-level manifest evidence association (one per batch_id)
CREATE TABLE IF NOT EXISTS raw.batch_evidence (
    batch_id VARCHAR(64) PRIMARY KEY REFERENCES raw.source_batches(batch_id),
    manifest_blob_hash VARCHAR(64) NOT NULL REFERENCES raw.evidence_blobs(blob_hash),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS idx_batch_evidence_manifest ON raw.batch_evidence(manifest_blob_hash);

COMMENT ON TABLE raw.batch_evidence IS
    'Associates a delivered batch_id with its exact manifest payload blob.';

-- 3. File delivery provenance associations (one per batch_id, source_type)
CREATE TABLE IF NOT EXISTS raw.batch_file_evidence (
    batch_id VARCHAR(64) NOT NULL REFERENCES raw.source_batches(batch_id),
    source_type VARCHAR(32) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    file_blob_hash VARCHAR(64) NOT NULL REFERENCES raw.evidence_blobs(blob_hash),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    PRIMARY KEY (batch_id, source_type)
);

CREATE INDEX IF NOT EXISTS idx_batch_file_evidence_blob ON raw.batch_file_evidence(file_blob_hash);

COMMENT ON TABLE raw.batch_file_evidence IS
    'Retains delivery occurrence provenance for each declared file in a batch, linking to shared content blobs.';

-- 4. Environment boundary enforcement and least-privilege role permissions
DO $$
DECLARE
    owner_role text;
    pipe_role text;
    dash_role text;
    foreign_role text;
BEGIN
    -- Identify active environment and require execution by environment owner
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
            RAISE EXCEPTION 'Migrations require explicit recon_dev or recon_test target database';
    END CASE;

    IF current_user <> owner_role THEN
        RAISE EXCEPTION 'Run migrations as designated environment owner (%)', owner_role;
    END IF;

    -- Grant SELECT, INSERT only to active environment pipeline role (no UPDATE, DELETE, TRUNCATE)
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = pipe_role) THEN
        EXECUTE format('GRANT SELECT, INSERT ON raw.evidence_blobs, raw.batch_evidence, raw.batch_file_evidence TO %I', pipe_role);
    END IF;

    -- Revoke all permissions from dashboard roles (dashboards read approved reporting views only, no raw byte access)
    FOREACH foreign_role IN ARRAY ARRAY['recon_dashboard', 'recon_test_dashboard'] LOOP
        IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = foreign_role) THEN
            EXECUTE format('REVOKE ALL ON raw.evidence_blobs, raw.batch_evidence, raw.batch_file_evidence FROM %I', foreign_role);
        END IF;
    END LOOP;

    -- Revoke all permissions from wrong-environment pipeline roles
    FOREACH foreign_role IN ARRAY ARRAY['recon_pipeline', 'recon_test_pipeline'] LOOP
        IF foreign_role <> pipe_role AND EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = foreign_role) THEN
            EXECUTE format('REVOKE ALL ON raw.evidence_blobs, raw.batch_evidence, raw.batch_file_evidence FROM %I', foreign_role);
        END IF;
    END LOOP;
END $$;
