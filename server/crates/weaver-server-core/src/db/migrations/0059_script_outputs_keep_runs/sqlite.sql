CREATE TABLE script_outputs_new (
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
    status TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    instance_id TEXT REFERENCES script_instances(id) ON DELETE SET NULL
);

INSERT INTO script_outputs_new (
    id, job_id, event, script, seq, raw_bytes, truncated, output,
    stored_bytes, result_json, status, created_at, instance_id
)
SELECT
    id, job_id, event, script, seq, raw_bytes, truncated, output,
    stored_bytes, result_json, status, created_at, instance_id
FROM script_outputs;

DROP TABLE script_outputs;
ALTER TABLE script_outputs_new RENAME TO script_outputs;

CREATE INDEX script_outputs_job_seq ON script_outputs(job_id, seq);
CREATE INDEX script_outputs_event_seq ON script_outputs(event, seq) WHERE job_id IS NULL;
CREATE INDEX script_outputs_status_seq ON script_outputs(status, seq);
CREATE INDEX script_outputs_instance ON script_outputs(instance_id);
