-- A graph checkpoint keeps the kind whose reading of a member its own bytes
-- refuted, so after a restart the member is claimed by its bytes and no set
-- is founded from its name again. Checkpoints before this migration kept
-- none.
ALTER TABLE graph_members ADD COLUMN refuted_kind TEXT;
