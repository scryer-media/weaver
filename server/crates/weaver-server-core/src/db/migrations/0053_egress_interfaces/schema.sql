CREATE TABLE egress_interfaces (
    id BIGINT PRIMARY KEY CHECK (id >= 0),
    name TEXT NOT NULL,
    binding_kind TEXT NOT NULL CHECK (binding_kind IN ('system', 'interface', 'sourceAddress')),
    binding_value TEXT,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    max_download_speed BIGINT NOT NULL CHECK (max_download_speed >= 0),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CHECK ((id = 0 AND name = 'System' AND binding_kind = 'system' AND binding_value IS NULL AND enabled = 1)
        OR (id > 0 AND binding_kind != 'system' AND binding_value IS NOT NULL))
);

INSERT INTO egress_interfaces
    (id, name, binding_kind, binding_value, enabled, max_download_speed, created_at, updated_at)
VALUES (0, 'System', 'system', NULL, 1, 0, 0, 0);

CREATE TABLE proxy_pools (
    id BIGINT PRIMARY KEY CHECK (id > 0),
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    member_ids TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
