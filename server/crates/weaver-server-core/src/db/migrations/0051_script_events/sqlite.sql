CREATE TABLE script_output_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_seq BIGINT NOT NULL DEFAULT 0,
    used_bytes BIGINT NOT NULL DEFAULT 0
);
INSERT INTO script_output_state(singleton) VALUES (1);
CREATE TABLE script_outputs (
    id TEXT PRIMARY KEY,
    job_id BIGINT,
    event TEXT NOT NULL,
    script TEXT NOT NULL,
    seq BIGINT NOT NULL UNIQUE,
    raw_bytes BIGINT NOT NULL,
    truncated BOOLEAN NOT NULL,
    output BLOB NOT NULL,
    stored_bytes BIGINT NOT NULL,
    result_json TEXT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX script_outputs_job_seq ON script_outputs(job_id, seq);
CREATE INDEX script_outputs_event_seq ON script_outputs(event, seq);
CREATE TABLE script_event_queue (
    run_id TEXT PRIMARY KEY,
    job_id BIGINT,
    event TEXT NOT NULL,
    priority INTEGER NOT NULL,
    seq BIGINT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('queued', 'started', 'done', 'interrupted')),
    payload TEXT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX script_event_queue_pending ON script_event_queue(state, priority, seq);
CREATE INDEX script_event_queue_job ON script_event_queue(job_id, event, state);
CREATE TABLE script_job_state (
    job_id BIGINT PRIMARY KEY,
    state TEXT NOT NULL
);
ALTER TABLE rss_feeds ADD COLUMN scripts TEXT NOT NULL DEFAULT '[]';
