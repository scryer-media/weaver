-- A hold past the budgets lies in its member's spill file at its own
-- offset, and the row says so: a restart reads it back from there, and
-- nothing of it is fetched again. Checkpoints before this migration kept
-- every hold on scratch.
ALTER TABLE graph_holds ADD COLUMN on_file INTEGER NOT NULL DEFAULT 0;
