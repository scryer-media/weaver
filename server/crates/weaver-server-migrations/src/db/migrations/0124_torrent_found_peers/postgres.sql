-- Of the peers a torrent starts from, those found elsewhere than its
-- trackers and the operator (the DHT, local discovery, peer exchange, and,
-- on a private torrent no tracker has answered yet, incoming ones), as JSON
-- like `peers`: a private torrent keeps them only until a tracker answers,
-- across a restart as within a run. NULL when there are none.
ALTER TABLE torrent_counters ADD COLUMN found_peers TEXT;
