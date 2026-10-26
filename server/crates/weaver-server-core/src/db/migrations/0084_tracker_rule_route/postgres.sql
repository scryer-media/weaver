-- Whether a tracker rule's torrents route through their own consumer.
ALTER TABLE tracker_rules ADD COLUMN torrent_route BOOLEAN NOT NULL DEFAULT FALSE;
