-- An RSS item a rule took while torrents were off is kept as 'waiting', not
-- as seen, and judged anew once torrents are on. This index finds those few
-- among the seen items, which grow with every feed's history, so turning
-- torrents on reads and writes only them. The state is matched as a literal.
CREATE INDEX idx_rss_seen_waiting ON rss_seen_items (feed_id) WHERE decision = 'waiting';
