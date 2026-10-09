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
CREATE TABLE script_instance_inputs (
    instance_id TEXT NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    secret BOOLEAN NOT NULL,
    position BIGINT NOT NULL,
    PRIMARY KEY (instance_id, name)
);
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
