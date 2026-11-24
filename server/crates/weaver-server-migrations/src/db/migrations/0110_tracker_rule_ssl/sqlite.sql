-- The identity a tracker issued for SSL torrents under a rule: its
-- certificate chain (PEM) and its private key, sealed with the database's
-- encryption key. Both NULL when the rule holds no identity.
ALTER TABLE tracker_rules ADD COLUMN ssl_certificate TEXT;
ALTER TABLE tracker_rules ADD COLUMN ssl_private_key TEXT;
