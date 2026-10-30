-- A tracker rule's one switch for whether its torrents look for peers
-- beyond their trackers becomes three: the DHT, peer exchange and local
-- discovery, each NULL to inherit. Each takes the one switch's value, so a
-- rule that held its torrents to their trackers still does.
ALTER TABLE tracker_rules ADD COLUMN dht BOOLEAN;
ALTER TABLE tracker_rules ADD COLUMN pex BOOLEAN;
ALTER TABLE tracker_rules ADD COLUMN lsd BOOLEAN;
UPDATE tracker_rules SET dht = discovery, pex = discovery, lsd = discovery;
ALTER TABLE tracker_rules DROP COLUMN discovery;
