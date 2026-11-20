-- The entries of a job's post-processing list whose turn has come, written
-- before each one starts. After a restart the ones listed are never run
-- again, and the rest of the list runs. NULL for a job whose scripts never
-- started under a weaver that kept the list.
ALTER TABLE active_jobs ADD COLUMN post_processing_started TEXT;
