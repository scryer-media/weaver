-- A tracker rule may say which transport its torrents dial peers on first:
-- tcp_first, utp_first or utp_only. Empty follows the global setting.
ALTER TABLE tracker_rules ADD COLUMN peer_protocol TEXT;
