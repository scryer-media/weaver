-- The pool member a torrent on a pool route is pinned to, by its proxy id;
-- NULL when its route has no pool.
ALTER TABLE torrents ADD COLUMN pool_member BIGINT;
