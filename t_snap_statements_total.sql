CREATE TABLE snap_statements_total (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    calls bigint,
    total_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    statements bigint,
    CONSTRAINT pk_snap_statements_total PRIMARY KEY (snap_id,dbid)
);
COMMENT ON TABLE snap_statements_total IS 'Aggregated stats for snapshot, based on pg_stat_statements';
