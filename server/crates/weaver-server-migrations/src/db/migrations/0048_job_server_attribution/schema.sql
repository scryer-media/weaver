-- Which servers served a finished job's articles, as compact JSON written by
-- the per-job attribution ledger. Reporting only: nothing reads it to make a
-- scheduling or failover decision, and a job that attributed nothing stores
-- NULL rather than an empty document.
ALTER TABLE job_history ADD COLUMN server_attribution TEXT;
