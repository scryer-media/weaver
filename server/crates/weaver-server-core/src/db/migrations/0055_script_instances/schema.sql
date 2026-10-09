CREATE TABLE script_instances (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    script TEXT NOT NULL,
    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('post_processing', 'queue', 'scan', 'schedule', 'feed')),
    trigger_detail TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL,
    blocking BOOLEAN NOT NULL,
    timeout_seconds BIGINT,
    run_order BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE INDEX script_instances_trigger ON script_instances(trigger_kind, trigger_detail, run_order);
CREATE TABLE secrets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_key TEXT NOT NULL UNIQUE,
    value TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE TABLE script_instance_inputs (
    instance_id TEXT NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    secret_id TEXT REFERENCES secrets(id) ON DELETE RESTRICT,
    position BIGINT NOT NULL,
    PRIMARY KEY (instance_id, name),
    CHECK (secret_id IS NULL OR value = '')
);
CREATE INDEX script_instance_inputs_secret ON script_instance_inputs(secret_id);
CREATE TABLE script_instance_categories (
    instance_id TEXT NOT NULL,
    category TEXT NOT NULL,
    PRIMARY KEY (instance_id, category)
);
CREATE TABLE feed_scripts (
    feed_id BIGINT NOT NULL,
    instance_id TEXT NOT NULL,
    run_order BIGINT NOT NULL,
    PRIMARY KEY (feed_id, instance_id)
);
CREATE INDEX feed_scripts_instance ON feed_scripts(instance_id);
