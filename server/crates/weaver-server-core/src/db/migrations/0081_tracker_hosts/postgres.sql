-- Every host a torrent has announced to, kept after the torrent is gone so a
-- tracker rule can be picked from them. Only the host: a private tracker's
-- URL carries the operator's passkey. Each host is stored once, as a rule
-- matches it: lower case, punycode, no port and no trailing dot.
CREATE TABLE tracker_hosts (
    host TEXT PRIMARY KEY CHECK (host <> '' AND host = lower(host))
);
