ALTER TABLE network_bandwidth ADD COLUMN usenet_split_pct BIGINT
    CHECK (usenet_split_pct >= 1 AND usenet_split_pct <= 99);
