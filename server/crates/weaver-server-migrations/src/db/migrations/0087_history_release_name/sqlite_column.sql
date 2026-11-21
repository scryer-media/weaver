-- The History page sorts and searches by a row's full release name, which
-- lives in its metadata. It is stored here so SQL can order by it through an
-- index; the step after this one fills it for the rows already written.
ALTER TABLE job_history ADD COLUMN release_name TEXT NOT NULL DEFAULT '';
