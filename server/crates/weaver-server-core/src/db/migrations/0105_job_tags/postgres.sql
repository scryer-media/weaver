-- Tags: free labels on a job, by its id, the same for a Usenet job and a
-- torrent. A job keeps its tags into history. Nothing reads a tag to decide
-- anything. A tag is matched as written: "TV" and "tv" are two tags.
CREATE TABLE job_tags (
    job_id BIGINT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (job_id, tag)
);

CREATE INDEX idx_job_tags_tag ON job_tags (tag, job_id);

-- Every tag made, whether or not a job carries it.
CREATE TABLE tags (
    name TEXT PRIMARY KEY NOT NULL
);
