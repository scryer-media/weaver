-- Whether a torrent's archives are extracted. A category, a tracker rule or
-- the torrent itself may set it; NULL inherits the next level up, and the
-- torrents settings hold the default.
ALTER TABLE categories ADD COLUMN unpack INTEGER;
ALTER TABLE tracker_rules ADD COLUMN unpack INTEGER;
ALTER TABLE torrents ADD COLUMN unpack INTEGER;
