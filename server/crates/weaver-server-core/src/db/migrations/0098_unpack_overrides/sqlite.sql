-- Whether a torrent's archives are extracted. A category, a tracker rule or
-- the torrent itself may set it; NULL inherits the next level up, and the
-- torrents settings hold the default.
ALTER TABLE categories ADD COLUMN unpack INTEGER;
ALTER TABLE tracker_rules ADD COLUMN unpack INTEGER;
-- A torrent keeps both: what is asked of it, and what the job its
-- archives were built under was given.
ALTER TABLE torrents ADD COLUMN unpack INTEGER;
ALTER TABLE torrents ADD COLUMN unpack_in_force INTEGER;
