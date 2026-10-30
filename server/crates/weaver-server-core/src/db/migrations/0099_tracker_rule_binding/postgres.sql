-- A tracker rule a tracker file filled: the file's name, revision and
-- import time, the fields it locked and the values it recommended, as
-- JSON; NULL when no file binds the rule. And the archive passwords the
-- file carried, sealed with the database's key; NULL for none.
ALTER TABLE tracker_rules ADD COLUMN bound TEXT;
ALTER TABLE tracker_rules ADD COLUMN archive_passwords TEXT;
