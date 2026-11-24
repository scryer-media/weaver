-- What a job's post-processing scripts are told about how its download
-- ended, written when the pass begins, so the scripts a restart resumes are
-- told the same. NULL for a job whose pass began under a weaver that did not
-- keep it.
ALTER TABLE active_jobs ADD COLUMN post_processing_outcome TEXT;
