CREATE TABLE IF NOT EXISTS diagnostic_runs (
    source_job_id INTEGER PRIMARY KEY NOT NULL,
    diagnostic_job_id INTEGER NOT NULL,
    smg_diagnostic_id TEXT,
    stage TEXT NOT NULL,
    include_server_hostnames INTEGER NOT NULL DEFAULT 1,
    rerun_succeeded INTEGER,
    error_message TEXT,
    created_at_epoch_ms INTEGER NOT NULL,
    updated_at_epoch_ms INTEGER NOT NULL,
    last_activity_at_epoch_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_diagnostic_runs_job
    ON diagnostic_runs(diagnostic_job_id);

CREATE INDEX IF NOT EXISTS idx_diagnostic_runs_activity
    ON diagnostic_runs(last_activity_at_epoch_ms, stage);

CREATE TABLE IF NOT EXISTS async_operations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,
    state        TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    requested_at INTEGER NOT NULL,
    started_at   INTEGER,
    finished_at  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_async_operations_kind_state_requested
    ON async_operations(kind, state, requested_at, id);

CREATE TABLE IF NOT EXISTS async_operation_targets (
    operation_id   INTEGER NOT NULL REFERENCES async_operations(id) ON DELETE CASCADE,
    target_kind    TEXT NOT NULL,
    target_id      INTEGER NOT NULL,
    state          TEXT NOT NULL,
    error_message  TEXT,
    sort_order     INTEGER NOT NULL,
    PRIMARY KEY (operation_id, target_kind, target_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_async_operation_targets_target_state
    ON async_operation_targets(target_kind, target_id, state);

CREATE INDEX IF NOT EXISTS idx_async_operation_targets_operation_sort
    ON async_operation_targets(operation_id, target_kind, sort_order, target_id);
