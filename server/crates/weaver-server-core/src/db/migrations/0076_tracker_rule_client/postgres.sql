-- The client a tracker rule's torrents name themselves as to its peers and
-- trackers; NULL is Weaver.
ALTER TABLE tracker_rules ADD COLUMN client TEXT;
