-- What a tracker rule holds its torrents to beside their seeding: whether
-- they look for peers through the DHT, through peer exchange and through
-- local discovery (each NULL inherits), the encryption their connections
-- are made under (NULL inherits), their upload cap (`off` for none or
-- bytes per second; NULL inherits), and whether one admitted under it
-- starts forced.
ALTER TABLE tracker_rules ADD COLUMN dht INTEGER;
ALTER TABLE tracker_rules ADD COLUMN pex INTEGER;
ALTER TABLE tracker_rules ADD COLUMN lsd INTEGER;
ALTER TABLE tracker_rules ADD COLUMN encryption TEXT;
ALTER TABLE tracker_rules ADD COLUMN upload_cap TEXT;
ALTER TABLE tracker_rules ADD COLUMN forced INTEGER NOT NULL DEFAULT 0;
