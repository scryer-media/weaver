ALTER TABLE servers ADD COLUMN max_download_speed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE servers ADD COLUMN download_quota_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE servers ADD COLUMN download_quota_limit_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE servers ADD COLUMN download_quota_period TEXT NOT NULL DEFAULT 'one_time';
ALTER TABLE servers ADD COLUMN download_quota_reset_time_minutes_local INTEGER NOT NULL DEFAULT 0;
ALTER TABLE servers ADD COLUMN download_quota_weekly_reset_weekday TEXT NOT NULL DEFAULT 'mon';
ALTER TABLE servers ADD COLUMN download_quota_monthly_reset_day INTEGER NOT NULL DEFAULT 1;

CREATE TABLE server_download_usage (
    server_id                  INTEGER PRIMARY KEY NOT NULL
                               REFERENCES servers(id) ON DELETE CASCADE,
    lifetime_bytes             INTEGER NOT NULL DEFAULT 0,
    quota_baseline_bytes       INTEGER NOT NULL DEFAULT 0,
    window_start_epoch_seconds INTEGER,
    window_end_epoch_seconds   INTEGER,
    updated_at_epoch_seconds   INTEGER NOT NULL DEFAULT 0
);
