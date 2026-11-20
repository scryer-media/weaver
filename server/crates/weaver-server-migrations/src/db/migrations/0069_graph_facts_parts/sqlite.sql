-- The sealed parts of a node's facts, each its own row, so a checkpoint
-- writes again only the parts its step changed. A node's facts are its
-- row's blob with every part of it here; the blob names each part by its
-- key.
CREATE TABLE graph_facts_parts (
    job_id   INTEGER NOT NULL,
    node_id  INTEGER NOT NULL,
    part     INTEGER NOT NULL,
    bytes    BLOB NOT NULL,
    PRIMARY KEY (job_id, node_id, part)
) WITHOUT ROWID;
