-- Reporting checkpoint only; never used to skip articles or choose providers.
ALTER TABLE active_jobs ADD COLUMN server_attribution TEXT;
