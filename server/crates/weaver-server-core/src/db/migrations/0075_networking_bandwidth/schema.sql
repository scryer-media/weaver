CREATE TABLE network_bandwidth (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    download_bps BIGINT CHECK (download_bps >= 0),
    upload_bps BIGINT CHECK (upload_bps >= 0)
);

INSERT INTO network_bandwidth (id, download_bps, upload_bps) VALUES (1, NULL, NULL);

ALTER TABLE egress_interfaces ADD COLUMN download_bps BIGINT CHECK (download_bps >= 0);
ALTER TABLE egress_interfaces ADD COLUMN upload_bps BIGINT CHECK (upload_bps >= 0);
ALTER TABLE egress_interfaces ADD COLUMN quota_download_bytes BIGINT CHECK (quota_download_bytes >= 0);
ALTER TABLE egress_interfaces ADD COLUMN quota_download_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE egress_interfaces ADD COLUMN quota_upload_soft_bytes BIGINT CHECK (quota_upload_soft_bytes >= 0);
ALTER TABLE egress_interfaces ADD COLUMN quota_upload_trickle_bps BIGINT NOT NULL DEFAULT 2048 CHECK (quota_upload_trickle_bps >= 0);
ALTER TABLE egress_interfaces ADD COLUMN quota_upload_hard_bytes BIGINT CHECK (quota_upload_hard_bytes >= 0);

UPDATE egress_interfaces
   SET download_bps = NULLIF(max_download_speed, 0),
       quota_download_bytes = download_quota_limit_bytes,
       quota_download_enabled = download_quota_enabled;

ALTER TABLE egress_interfaces RENAME COLUMN download_quota_period TO quota_period;
ALTER TABLE egress_interfaces RENAME COLUMN download_quota_reset_time_minutes_local TO quota_reset_time_minutes_local;
ALTER TABLE egress_interfaces RENAME COLUMN download_quota_weekly_reset_weekday TO quota_weekly_reset_weekday;
ALTER TABLE egress_interfaces RENAME COLUMN download_quota_monthly_reset_day TO quota_monthly_reset_day;
ALTER TABLE egress_interfaces DROP COLUMN max_download_speed;
ALTER TABLE egress_interfaces DROP COLUMN download_quota_enabled;
ALTER TABLE egress_interfaces DROP COLUMN download_quota_limit_bytes;

ALTER TABLE egress_download_usage ADD COLUMN upload_lifetime_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE egress_download_usage ADD COLUMN upload_quota_baseline_bytes BIGINT NOT NULL DEFAULT 0;
