-- Every job runs on the graph engine, so a job no longer records which
-- engine runs it.
ALTER TABLE active_jobs DROP COLUMN engine;
ALTER TABLE job_history DROP COLUMN engine;

-- The graph engine keeps a job's restart state in its own checkpoint, so
-- nothing reads these tables any more.
DROP TABLE active_par2;
DROP TABLE active_par2_files;
DROP TABLE active_failed_extractions;
DROP TABLE active_extraction_chunks;
DROP TABLE active_archive_headers;
DROP TABLE active_rar_volume_facts;
DROP TABLE active_volume_status;
DROP TABLE active_rar_verified_suspect;
DROP TABLE active_direct_coverage;
DROP TABLE active_repair_outputs;
