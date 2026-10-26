-- A hold paged to scratch is a span of its namespace's one scratch file,
-- named by the node: the row keeps where in that file the span starts. A
-- hold in memory is paged out at a graceful stop, and one still in memory
-- is fetched again. Nothing wrote the table before this migration.
DROP TABLE graph_holds;
CREATE TABLE graph_holds (
    job_id          INTEGER NOT NULL,
    node_id         INTEGER NOT NULL,
    volume          INTEGER NOT NULL,
    range_start     INTEGER NOT NULL,
    range_end       INTEGER NOT NULL,
    scratch_offset  INTEGER NOT NULL,
    PRIMARY KEY (job_id, node_id, volume, range_start)
) WITHOUT ROWID;
