CREATE TABLE bandwidth_usage_metered_buckets (
    bucket_epoch_minute INTEGER NOT NULL,
    metered INTEGER NOT NULL DEFAULT 1 CHECK (metered IN (0, 1)),
    payload_bytes INTEGER NOT NULL,
    PRIMARY KEY (bucket_epoch_minute, metered)
) WITHOUT ROWID;
INSERT INTO bandwidth_usage_metered_buckets (bucket_epoch_minute, payload_bytes)
SELECT bucket_epoch_minute, payload_bytes FROM bandwidth_usage_minute_buckets;
DROP TABLE bandwidth_usage_minute_buckets;
ALTER TABLE bandwidth_usage_metered_buckets RENAME TO bandwidth_usage_minute_buckets;
