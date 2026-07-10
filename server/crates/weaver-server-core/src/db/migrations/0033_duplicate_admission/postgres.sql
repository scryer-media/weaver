CREATE TABLE duplicate_job_snapshots (
    job_id BIGINT PRIMARY KEY,
    lifecycle TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    admission_action TEXT NOT NULL,
    admission_reason TEXT,
    origin TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    reservation_expires_at BIGINT
);

CREATE TABLE job_fingerprints (
    job_id BIGINT NOT NULL REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE,
    fingerprint_kind TEXT NOT NULL,
    fingerprint_version BIGINT NOT NULL,
    fingerprint_digest BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (job_id, fingerprint_kind, fingerprint_version)
);

CREATE INDEX idx_job_fingerprints_lookup
    ON job_fingerprints (fingerprint_kind, fingerprint_version, fingerprint_digest);

CREATE TABLE duplicate_admission_claims (
    fingerprint_kind TEXT NOT NULL,
    fingerprint_version BIGINT NOT NULL,
    fingerprint_digest BYTEA NOT NULL,
    job_id BIGINT NOT NULL REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE,
    claimed_at BIGINT NOT NULL,
    PRIMARY KEY (fingerprint_kind, fingerprint_version, fingerprint_digest)
);

CREATE INDEX idx_duplicate_admission_claims_job
    ON duplicate_admission_claims (job_id);

CREATE TABLE submission_idempotency (
    caller_scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    job_id BIGINT NOT NULL REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (caller_scope, idempotency_key)
);

CREATE INDEX idx_submission_idempotency_job ON submission_idempotency (job_id);

-- A destructive duplicate-identity forget must survive resumable backfill.
CREATE TABLE forgotten_duplicate_identities (
    job_id BIGINT PRIMARY KEY,
    forgotten_at BIGINT NOT NULL
);

CREATE TABLE duplicate_backfill_state (
    backfill_key TEXT PRIMARY KEY,
    cursor_job_id BIGINT,
    completed_at BIGINT,
    updated_at BIGINT NOT NULL
);

CREATE TABLE semantic_duplicate_groups (
    group_id BIGSERIAL PRIMARY KEY,
    normalized_key TEXT NOT NULL UNIQUE,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE semantic_duplicate_candidates (
    job_id BIGINT PRIMARY KEY REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES semantic_duplicate_groups(group_id) ON DELETE CASCADE,
    score BIGINT NOT NULL,
    candidate_state TEXT NOT NULL,
    source_nzb_zstd BYTEA,
    source_filename TEXT,
    source_password TEXT,
    source_category TEXT,
    source_metadata_json TEXT,
    terminal_cause TEXT,
    materialization_generation BIGINT NOT NULL DEFAULT 0,
    promotion_state TEXT NOT NULL DEFAULT 'none',
    promotion_generation BIGINT NOT NULL DEFAULT 0,
    promotion_claimed_at BIGINT,
    promotion_lease_expires_at BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX idx_semantic_duplicate_candidates_group_state_score
    ON semantic_duplicate_candidates (group_id, candidate_state, score DESC, job_id ASC);

CREATE INDEX idx_semantic_duplicate_candidates_promotion
    ON semantic_duplicate_candidates (promotion_state, group_id, score DESC, created_at ASC, job_id ASC);
