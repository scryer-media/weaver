-- A graph checkpoint keeps the checksum a member's object said for it, so a
-- restored member is checked as it was before the restart. Checkpoints
-- before this migration kept none.
ALTER TABLE graph_members ADD COLUMN crc32 BIGINT;
