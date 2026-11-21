-- A graph job's checkpoint: its tree as rows, every row keyed by job and the
-- job's rows replaced together in one transaction per checkpoint, so a
-- restore never reads half of one. They describe only bytes on disk.

-- One row per node. The root's facts are the article root's own blob; every
-- other node's are its object's, versioned per kind.
CREATE TABLE graph_nodes (
    job_id            INTEGER NOT NULL,
    node_id           INTEGER NOT NULL,
    parent_node_id    INTEGER,
    -- The parent member that is this node's first volume.
    parent_member_id  INTEGER,
    -- Every (volume, parent member) pair, encoded, for multi-volume objects.
    volumes           BLOB,
    window_start      INTEGER,
    window_end        INTEGER,
    kind              TEXT NOT NULL,
    state             TEXT NOT NULL,
    reason            TEXT,
    facts_version     INTEGER NOT NULL,
    facts             BLOB NOT NULL,
    winning_password  INTEGER,
    PRIMARY KEY (job_id, node_id)
) WITHOUT ROWID;

CREATE TABLE graph_members (
    job_id            INTEGER NOT NULL,
    node_id           INTEGER NOT NULL,
    member_id         INTEGER NOT NULL,
    name              TEXT NOT NULL,
    output_name       TEXT NOT NULL,
    length            INTEGER,
    closed            INTEGER NOT NULL,
    bound_node_id     INTEGER,
    owner_node_id     INTEGER,
    owner_volume      INTEGER,
    given_up_node_id  INTEGER,
    given_up_volume   INTEGER,
    undecided         INTEGER NOT NULL,
    has_file          INTEGER NOT NULL,
    refused           TEXT,
    -- Its place among the members carrying recovery data no object here
    -- reads, in the order the job found them.
    unusable_at       INTEGER,
    par2_set          BLOB,
    par2_file_id      BLOB,
    PRIMARY KEY (job_id, node_id, member_id)
) WITHOUT ROWID;

CREATE TABLE graph_coverage (
    job_id       INTEGER NOT NULL,
    node_id      INTEGER NOT NULL,
    member_id    INTEGER NOT NULL,
    range_start  INTEGER NOT NULL,
    range_end    INTEGER NOT NULL,
    provenance   TEXT NOT NULL,
    verified     INTEGER NOT NULL,
    PRIMARY KEY (job_id, node_id, member_id, range_start)
) WITHOUT ROWID;

-- Which ranges of each kept envelope are on disk. The bytes themselves stay
-- in the envelope's file, so `bytes` is only for a remainder kept nowhere
-- else.
CREATE TABLE graph_envelopes (
    job_id       INTEGER NOT NULL,
    node_id      INTEGER NOT NULL,
    volume       INTEGER NOT NULL,
    range_start  INTEGER NOT NULL,
    range_end    INTEGER NOT NULL,
    provenance   TEXT NOT NULL,
    closed       INTEGER,
    settled      INTEGER NOT NULL,
    bytes        BLOB,
    PRIMARY KEY (job_id, node_id, volume, range_start)
) WITHOUT ROWID;

-- Cipher chain seeds, never key material.
CREATE TABLE graph_crypt (
    job_id     INTEGER NOT NULL,
    node_id    INTEGER NOT NULL,
    member_id  INTEGER NOT NULL,
    run_start  INTEGER NOT NULL,
    seed       BLOB NOT NULL,
    PRIMARY KEY (job_id, node_id, member_id, run_start)
) WITHOUT ROWID;

CREATE TABLE graph_recovery (
    job_id            INTEGER NOT NULL,
    node_id           INTEGER NOT NULL,
    set_id            BLOB NOT NULL,
    slice_size        INTEGER NOT NULL,
    index_digest      BLOB,
    blocks_reachable  INTEGER NOT NULL,
    verdicts          BLOB,
    PRIMARY KEY (job_id, node_id, set_id)
) WITHOUT ROWID;

-- Only holds paged to scratch; a hold in memory is fetched again.
CREATE TABLE graph_holds (
    job_id        INTEGER NOT NULL,
    node_id       INTEGER NOT NULL,
    volume        INTEGER NOT NULL,
    range_start   INTEGER NOT NULL,
    range_end     INTEGER NOT NULL,
    scratch_path  TEXT NOT NULL,
    PRIMARY KEY (job_id, node_id, volume, range_start)
) WITHOUT ROWID;
