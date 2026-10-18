-- A history row an NZBGet-compatible client removed with HistoryDelete. The
-- row stays for duplicate checks and the web UI; only the facade's default
-- history listing leaves it out.
ALTER TABLE job_history ADD COLUMN nzbget_hidden BIGINT NOT NULL DEFAULT 0 CHECK (nzbget_hidden IN (0, 1));
