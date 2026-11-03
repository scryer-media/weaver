-- A graph job's checkpoint: its tree as rows, every row keyed by job and the
-- job's rows replaced together in one transaction per checkpoint, so a
-- restore never reads half of one. They describe only bytes on disk.

-- One row per node. The root's facts are the article root's own blob; every
-- other node's are its object's, versioned per kind.
CREATE TABLE graph_nodes (
    job_id            BIGINT NOT NULL,
    node_id           BIGINT NOT NULL,
    parent_node_id    BIGINT,
    -- The parent member that is this node's first volume.
    parent_member_id  BIGINT,
    -- Every (volume, parent member) pair, encoded, for multi-volume objects.
    volumes           BYTEA,
    window_start      BIGINT,
    window_end        BIGINT,
    kind              TEXT NOT NULL,
    state             TEXT NOT NULL,
    reason            TEXT,
    facts_version     BIGINT NOT NULL,
    facts             BYTEA NOT NULL,
    winning_password  BIGINT,
    PRIMARY KEY (job_id, node_id)
);

CREATE TABLE graph_members (
    job_id            BIGINT NOT NULL,
    node_id           BIGINT NOT NULL,
    member_id         BIGINT NOT NULL,
    name              TEXT NOT NULL,
    output_name       TEXT NOT NULL,
    length            BIGINT,
    closed            BOOLEAN NOT NULL,
    bound_node_id     BIGINT,
    owner_node_id     BIGINT,
    owner_volume      BIGINT,
    given_up_node_id  BIGINT,
    given_up_volume   BIGINT,
    undecided         BOOLEAN NOT NULL,
    has_file          BOOLEAN NOT NULL,
    refused           TEXT,
    -- Its place among the members carrying recovery data no object here
    -- reads, in the order the job found them.
    unusable_at       BIGINT,
    par2_set          BYTEA,
    par2_file_id      BYTEA,
    PRIMARY KEY (job_id, node_id, member_id)
);

CREATE TABLE graph_coverage (
    job_id       BIGINT NOT NULL,
    node_id      BIGINT NOT NULL,
    member_id    BIGINT NOT NULL,
    range_start  BIGINT NOT NULL,
    range_end    BIGINT NOT NULL,
    provenance   TEXT NOT NULL,
    verified     BOOLEAN NOT NULL,
    PRIMARY KEY (job_id, node_id, member_id, range_start)
);

-- Which ranges of each kept envelope are on disk. The bytes themselves stay
-- in the envelope's file, so `bytes` is only for a remainder kept nowhere
-- else.
CREATE TABLE graph_envelopes (
    job_id       BIGINT NOT NULL,
    node_id      BIGINT NOT NULL,
    volume       BIGINT NOT NULL,
    range_start  BIGINT NOT NULL,
    range_end    BIGINT NOT NULL,
    provenance   TEXT NOT NULL,
    closed       BIGINT,
    settled      BOOLEAN NOT NULL,
    bytes        BYTEA,
    PRIMARY KEY (job_id, node_id, volume, range_start)
);

-- Cipher chain seeds, never key material.
CREATE TABLE graph_crypt (
    job_id     BIGINT NOT NULL,
    node_id    BIGINT NOT NULL,
    member_id  BIGINT NOT NULL,
    run_start  BIGINT NOT NULL,
    seed       BYTEA NOT NULL,
    PRIMARY KEY (job_id, node_id, member_id, run_start)
);

CREATE TABLE graph_recovery (
    job_id            BIGINT NOT NULL,
    node_id           BIGINT NOT NULL,
    set_id            BYTEA NOT NULL,
    slice_size        BIGINT NOT NULL,
    index_digest      BYTEA,
    blocks_reachable  BIGINT NOT NULL,
    verdicts          BYTEA,
    PRIMARY KEY (job_id, node_id, set_id)
);

-- Only holds paged to scratch; a hold in memory is fetched again.
CREATE TABLE graph_holds (
    job_id        BIGINT NOT NULL,
    node_id       BIGINT NOT NULL,
    volume        BIGINT NOT NULL,
    range_start   BIGINT NOT NULL,
    range_end     BIGINT NOT NULL,
    scratch_path  TEXT NOT NULL,
    PRIMARY KEY (job_id, node_id, volume, range_start)
);
