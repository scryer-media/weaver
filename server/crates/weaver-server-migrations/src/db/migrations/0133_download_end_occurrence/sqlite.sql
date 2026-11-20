-- The occurrence of a job's download end: 0 for its first, and one more
-- for each end after it, as a torrent reopened and complete again has. A
-- queued end is admitted only past the occurrence of the job's latest one,
-- so an NZB job, whose download ends once, keeps one end. A torrent keeps
-- how many ends it has had, so a restart numbers its next one on. Zero for
-- every row written before this: nothing to backfill.
ALTER TABLE script_event_queue ADD COLUMN occurrence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE torrents ADD COLUMN download_ends INTEGER NOT NULL DEFAULT 0;
