ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS max_download_speed BIGINT NOT NULL DEFAULT 0;
ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS download_quota_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS download_quota_limit_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS download_quota_period TEXT NOT NULL DEFAULT 'one_time';
ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS download_quota_reset_time_minutes_local BIGINT NOT NULL DEFAULT 0;
ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS download_quota_weekly_reset_weekday TEXT NOT NULL DEFAULT 'mon';
ALTER TABLE servers
    ADD COLUMN IF NOT EXISTS download_quota_monthly_reset_day BIGINT NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS server_download_usage (
    server_id                  BIGINT PRIMARY KEY NOT NULL
                               REFERENCES servers(id) ON DELETE CASCADE,
    lifetime_bytes             BIGINT NOT NULL DEFAULT 0,
    quota_baseline_bytes       BIGINT NOT NULL DEFAULT 0,
    window_start_epoch_seconds BIGINT,
    window_end_epoch_seconds   BIGINT,
    updated_at_epoch_seconds   BIGINT NOT NULL DEFAULT 0
);
