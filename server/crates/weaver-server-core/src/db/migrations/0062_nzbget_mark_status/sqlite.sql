-- How an NZBGet-compatible client marked a history row: BAD, GOOD or
-- SUCCESS. NULL when it never marked it.
ALTER TABLE job_history ADD COLUMN nzbget_mark_status TEXT CHECK (nzbget_mark_status IN ('BAD', 'GOOD', 'SUCCESS'));
