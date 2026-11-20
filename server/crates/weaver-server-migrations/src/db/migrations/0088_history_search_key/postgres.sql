-- The History search finds its text in any of a row's titles, accents, case
-- and separators aside. Rust builds that key from the row's name and
-- metadata; it is stored here, and the step after this one fills it.
ALTER TABLE job_history ADD COLUMN search_key TEXT NOT NULL DEFAULT '';
