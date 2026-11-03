-- Where a torrent was asked to go: the directory its files go under, NULL
-- for its category's, and how they are laid out there, NULL for a folder of
-- its own. Once its metainfo is in hand, the directory its files sit in
-- when that is not its storage directory, NULL when it is; whether it was
-- added in seed mode and has not left it; and the directory it moves under
-- once complete, NULL when it stays.
ALTER TABLE torrents ADD COLUMN save_path TEXT;
ALTER TABLE torrents ADD COLUMN content_layout TEXT;
ALTER TABLE torrents ADD COLUMN files_dir TEXT;
ALTER TABLE torrents ADD COLUMN seed_mode INTEGER NOT NULL DEFAULT 0;
ALTER TABLE torrents ADD COLUMN move_to TEXT;
