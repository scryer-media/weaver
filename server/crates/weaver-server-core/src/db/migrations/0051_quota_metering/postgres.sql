ALTER TABLE bandwidth_usage_minute_buckets
    ADD COLUMN metered BIGINT NOT NULL DEFAULT 1 CHECK (metered IN (0, 1));
ALTER TABLE bandwidth_usage_minute_buckets DROP CONSTRAINT bandwidth_usage_minute_buckets_pkey;
ALTER TABLE bandwidth_usage_minute_buckets ADD PRIMARY KEY (bucket_epoch_minute, metered);
