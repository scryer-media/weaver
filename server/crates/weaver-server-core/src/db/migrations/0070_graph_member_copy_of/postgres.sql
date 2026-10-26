-- A member posted again under a name of its own is folded into the member
-- it repeats: its bytes are that member's. Checkpoints before this
-- migration folded none.
ALTER TABLE graph_members ADD COLUMN copy_of BIGINT;
