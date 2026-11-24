-- A tracker rule may turn TCP and uTP on or off for its torrents' outgoing
-- connections, and name the transport each tries first while downloading
-- and while seeding: tcp or utp. Empty follows the global setting.
ALTER TABLE tracker_rules ADD COLUMN peer_tcp BOOLEAN;
ALTER TABLE tracker_rules ADD COLUMN peer_utp BOOLEAN;
ALTER TABLE tracker_rules ADD COLUMN prefer_downloading TEXT;
ALTER TABLE tracker_rules ADD COLUMN prefer_seeding TEXT;

-- The single peer protocol setting is replaced by the transports.
DELETE FROM settings WHERE key = 'torrent.peer_protocol';
