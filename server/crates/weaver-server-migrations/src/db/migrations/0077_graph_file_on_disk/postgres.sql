-- A graph checkpoint keeps the name a member's file sits at while the file
-- waits to move to its output name, so a restore opens it where it is.
-- Empty when the file sits at its output name, as every file did before
-- this migration.
ALTER TABLE graph_members ADD COLUMN on_disk TEXT;

-- It also keeps the file an envelope sits in when that is not the one its
-- node and number name, because a member given back from it still sat
-- there when the envelope was kept again. Empty for every other envelope.
ALTER TABLE graph_envelopes ADD COLUMN file TEXT;
