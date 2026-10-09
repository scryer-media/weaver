CREATE TABLE egress_interfaces (
    id BIGINT PRIMARY KEY CHECK (id >= 0),
    name TEXT NOT NULL,
    binding_kind TEXT NOT NULL CHECK (binding_kind IN ('system', 'interface', 'sourceAddress')),
    binding_value TEXT,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    max_download_speed BIGINT NOT NULL CHECK (max_download_speed >= 0),
    download_quota_enabled INTEGER NOT NULL DEFAULT 0 CHECK (download_quota_enabled IN (0, 1)),
    download_quota_limit_bytes BIGINT NOT NULL DEFAULT 0 CHECK (download_quota_limit_bytes >= 0),
    download_quota_period TEXT NOT NULL DEFAULT 'one_time',
    download_quota_reset_time_minutes_local BIGINT NOT NULL DEFAULT 0,
    download_quota_weekly_reset_weekday TEXT NOT NULL DEFAULT 'mon',
    download_quota_monthly_reset_day BIGINT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CHECK ((id = 0 AND name = 'System' AND binding_kind = 'system' AND binding_value IS NULL AND enabled = 1)
        OR (id > 0 AND binding_kind != 'system' AND binding_value IS NOT NULL))
);

INSERT INTO egress_interfaces
    (id, name, binding_kind, binding_value, enabled, max_download_speed, created_at, updated_at)
VALUES (0, 'System', 'system', NULL, 1, 0, 0, 0);

CREATE TABLE egress_download_usage (
    egress_id                  BIGINT PRIMARY KEY NOT NULL
                               REFERENCES egress_interfaces(id) ON DELETE CASCADE,
    lifetime_bytes             BIGINT NOT NULL DEFAULT 0,
    quota_baseline_bytes       BIGINT NOT NULL DEFAULT 0,
    window_start_epoch_seconds BIGINT,
    window_end_epoch_seconds   BIGINT,
    updated_at_epoch_seconds   BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE proxy_pools (
    id BIGINT PRIMARY KEY CHECK (id > 0),
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    member_ids TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
