CREATE TABLE duplicate_job_snapshots (
    job_id INTEGER PRIMARY KEY,
    lifecycle TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    admission_action TEXT NOT NULL,
    admission_reason TEXT,
    origin TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    reservation_expires_at INTEGER
);

CREATE TABLE job_fingerprints (
    job_id INTEGER NOT NULL,
    fingerprint_kind TEXT NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    fingerprint_digest BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, fingerprint_kind, fingerprint_version),
    FOREIGN KEY (job_id) REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE
);

CREATE INDEX idx_job_fingerprints_lookup
    ON job_fingerprints (fingerprint_kind, fingerprint_version, fingerprint_digest);

CREATE TABLE duplicate_admission_claims (
    fingerprint_kind TEXT NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    fingerprint_digest BLOB NOT NULL,
    job_id INTEGER NOT NULL,
    claimed_at INTEGER NOT NULL,
    PRIMARY KEY (fingerprint_kind, fingerprint_version, fingerprint_digest),
    FOREIGN KEY (job_id) REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE
);

CREATE INDEX idx_duplicate_admission_claims_job
    ON duplicate_admission_claims (job_id);

CREATE TABLE submission_idempotency (
    caller_scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BLOB NOT NULL,
    job_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (caller_scope, idempotency_key),
    FOREIGN KEY (job_id) REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE
);

CREATE INDEX idx_submission_idempotency_job ON submission_idempotency (job_id);

-- A destructive duplicate-identity forget must survive resumable backfill.
CREATE TABLE forgotten_duplicate_identities (
    job_id INTEGER PRIMARY KEY,
    forgotten_at INTEGER NOT NULL
);

CREATE TABLE duplicate_backfill_state (
    backfill_key TEXT PRIMARY KEY,
    cursor_job_id INTEGER,
    completed_at INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE TABLE semantic_duplicate_groups (
    group_id INTEGER PRIMARY KEY,
    normalized_key TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE semantic_duplicate_candidates (
    job_id INTEGER PRIMARY KEY,
    group_id INTEGER NOT NULL,
    score INTEGER NOT NULL,
    candidate_state TEXT NOT NULL,
    source_nzb_zstd BLOB,
    source_filename TEXT,
    source_password TEXT,
    source_category TEXT,
    source_metadata_json TEXT,
    terminal_cause TEXT,
    materialization_generation INTEGER NOT NULL DEFAULT 0,
    promotion_state TEXT NOT NULL DEFAULT 'none',
    promotion_generation INTEGER NOT NULL DEFAULT 0,
    promotion_claimed_at INTEGER,
    promotion_lease_expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY (job_id) REFERENCES duplicate_job_snapshots(job_id) ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES semantic_duplicate_groups(group_id) ON DELETE CASCADE
);

CREATE INDEX idx_semantic_duplicate_candidates_group_state_score
    ON semantic_duplicate_candidates (group_id, candidate_state, score DESC, job_id ASC);

CREATE INDEX idx_semantic_duplicate_candidates_promotion
    ON semantic_duplicate_candidates (promotion_state, group_id, score DESC, created_at ASC, job_id ASC);
