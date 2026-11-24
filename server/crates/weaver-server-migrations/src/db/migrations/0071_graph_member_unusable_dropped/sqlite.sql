-- Every recovery record inside a volume has an object that reads it, so no
-- member is recorded as carrying recovery data nothing reads.
ALTER TABLE graph_members DROP COLUMN unusable_at;
