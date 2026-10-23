-- A member a retired object gave up and the owner then buried is settled:
-- no object bound later is offered it. Checkpoints before this migration
-- said nothing of it.
ALTER TABLE graph_members ADD COLUMN buried BOOLEAN NOT NULL DEFAULT FALSE;
