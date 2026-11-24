CREATE TABLE post_processing_extension_revisions (
    extension_id          TEXT NOT NULL,
    revision_id           TEXT NOT NULL,
    declared_version      TEXT NOT NULL,
    digest                TEXT NOT NULL,
    adapter               TEXT NOT NULL,
    display_name          TEXT NOT NULL,
    compatibility_name    TEXT,
    entrypoint            TEXT NOT NULL,
    manifest_json         TEXT NOT NULL,
    managed_path          TEXT,
    discovered_source_path TEXT,
    trust_state           TEXT NOT NULL DEFAULT 'unapproved',
    discovered_at_epoch_ms BIGINT NOT NULL,
    approved_at_epoch_ms  BIGINT,
    PRIMARY KEY (extension_id, revision_id),
    UNIQUE (digest)
);

CREATE INDEX idx_post_processing_revisions_extension_trust
    ON post_processing_extension_revisions(extension_id, trust_state, discovered_at_epoch_ms DESC);

CREATE TABLE post_processing_profiles (
    profile_id          TEXT PRIMARY KEY NOT NULL,
    name                TEXT NOT NULL,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at_epoch_ms BIGINT NOT NULL,
    updated_at_epoch_ms BIGINT NOT NULL
);

CREATE TABLE post_processing_profile_steps (
    profile_id          TEXT NOT NULL,
    step_index          BIGINT NOT NULL,
    selection_json      TEXT NOT NULL,
    policy_json         TEXT NOT NULL,
    options_json        TEXT NOT NULL,
    secret_options_json TEXT NOT NULL,
    PRIMARY KEY (profile_id, step_index),
    FOREIGN KEY (profile_id) REFERENCES post_processing_profiles(profile_id) ON DELETE CASCADE
);

CREATE TABLE post_processing_profile_assignments (
    scope_kind          TEXT NOT NULL,
    scope_key           TEXT NOT NULL,
    profile_id          TEXT NOT NULL,
    PRIMARY KEY (scope_kind, scope_key),
    FOREIGN KEY (profile_id) REFERENCES post_processing_profiles(profile_id) ON DELETE CASCADE
);

CREATE TABLE post_processing_job_plans (
    job_id              BIGINT PRIMARY KEY NOT NULL,
    provenance_json     TEXT NOT NULL,
    plan_json           TEXT NOT NULL,
    secret_options_json TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL
);

CREATE TABLE post_processing_runs (
    run_id               TEXT PRIMARY KEY NOT NULL,
    job_id               BIGINT NOT NULL,
    status               TEXT NOT NULL,
    pipeline_outcome_json TEXT NOT NULL,
    summary              TEXT NOT NULL DEFAULT 'not_run',
    terminal_intent      TEXT NOT NULL,
    plan_json            TEXT NOT NULL,
    secret_options_json  TEXT NOT NULL,
    rerun_of_run_id      TEXT,
    queued_at_epoch_ms   BIGINT NOT NULL,
    queue_position       BIGINT NOT NULL,
    started_at_epoch_ms  BIGINT,
    finished_at_epoch_ms BIGINT,
    FOREIGN KEY (rerun_of_run_id) REFERENCES post_processing_runs(run_id)
);

CREATE INDEX idx_post_processing_runs_job_queued
    ON post_processing_runs(job_id, queued_at_epoch_ms DESC);
CREATE INDEX idx_post_processing_runs_status_queued
    ON post_processing_runs(status, queue_position);

CREATE TABLE post_processing_attempts (
    attempt_id           TEXT PRIMARY KEY NOT NULL,
    run_id               TEXT NOT NULL,
    step_index           BIGINT NOT NULL,
    status               TEXT NOT NULL,
    extension_id         TEXT NOT NULL,
    revision_id          TEXT NOT NULL,
    adapter              TEXT NOT NULL,
    command_json         TEXT,
    working_directory    TEXT,
    exit_code            BIGINT,
    error_message        TEXT,
    progress_json        TEXT,
    control_token_hash   BYTEA,
    output_truncated     BOOLEAN NOT NULL DEFAULT FALSE,
    queued_at_epoch_ms   BIGINT NOT NULL,
    started_at_epoch_ms  BIGINT,
    finished_at_epoch_ms BIGINT,
    UNIQUE (run_id, step_index),
    FOREIGN KEY (run_id) REFERENCES post_processing_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX idx_post_processing_attempts_run_step
    ON post_processing_attempts(run_id, step_index);
CREATE INDEX idx_post_processing_attempts_status_queued
    ON post_processing_attempts(status, queued_at_epoch_ms);

CREATE TABLE post_processing_log_chunks (
    attempt_id           TEXT NOT NULL,
    sequence             BIGINT NOT NULL,
    stream               TEXT NOT NULL,
    payload              BYTEA NOT NULL,
    byte_count           BIGINT NOT NULL,
    created_at_epoch_ms  BIGINT NOT NULL,
    PRIMARY KEY (attempt_id, sequence),
    FOREIGN KEY (attempt_id) REFERENCES post_processing_attempts(attempt_id) ON DELETE CASCADE
);

ALTER TABLE active_jobs ADD COLUMN pipeline_outcome_json TEXT;
ALTER TABLE active_jobs ADD COLUMN post_processing_summary TEXT NOT NULL DEFAULT 'not_run';
ALTER TABLE active_jobs ADD COLUMN post_processing_run_id TEXT;

ALTER TABLE job_history ADD COLUMN pipeline_outcome_json TEXT;
ALTER TABLE job_history ADD COLUMN post_processing_summary TEXT NOT NULL DEFAULT 'not_run';
ALTER TABLE job_history ADD COLUMN post_processing_run_id TEXT;
