-- Where a job stages the files it delivers, on the device of its
-- destination, once it has begun. NULL when the job stages nothing there.
ALTER TABLE active_jobs ADD COLUMN staging_dir TEXT;
