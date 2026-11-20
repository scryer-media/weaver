-- A torrent keeps the levels the operator set on its files, one digit per
-- file in the metainfo's order (0 ignored, 1 normal, 6 high, 7 maximum),
-- NULL when every file is normal; whether it fetches its pieces in order;
-- and whether each file's first and last pieces go first. The file rows go:
-- their path and length are the metainfo's, and `selected` only ever held
-- whether the file was padding, which the metainfo also says.
ALTER TABLE torrents ADD COLUMN file_priorities TEXT;
ALTER TABLE torrents ADD COLUMN sequential INTEGER NOT NULL DEFAULT 0;
ALTER TABLE torrents ADD COLUMN first_last_piece_first INTEGER NOT NULL DEFAULT 0;
DROP TABLE torrent_files;
