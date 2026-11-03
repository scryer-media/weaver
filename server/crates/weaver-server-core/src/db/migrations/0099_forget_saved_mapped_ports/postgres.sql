-- A port a gateway maps lives as long as the session that holds the
-- mapping; none is kept across restarts.
DELETE FROM settings WHERE key = 'torrent.mapped_ports';
