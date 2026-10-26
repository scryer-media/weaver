-- What a tracker rule holds its torrents to beside their seeding: whether
-- they look for peers beyond their trackers (NULL inherits), the encryption
-- their connections are made under (NULL inherits), their upload cap
-- (`unlimited` or bytes per second; NULL inherits), and whether one
-- admitted under it starts forced.
ALTER TABLE tracker_rules ADD COLUMN discovery INTEGER;
ALTER TABLE tracker_rules ADD COLUMN encryption TEXT;
ALTER TABLE tracker_rules ADD COLUMN upload_limit TEXT;
ALTER TABLE tracker_rules ADD COLUMN forced INTEGER NOT NULL DEFAULT 0;
