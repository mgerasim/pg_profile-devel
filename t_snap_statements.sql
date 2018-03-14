CREATE TABLE snap_statements (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    userid oid,
    dbid oid,
    queryid bigint,
    queryid_md5 char(10) REFERENCES stmt_list (queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    calls bigint,
    total_time double precision,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    stddev_time double precision,
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
    CONSTRAINT pk_snap_statements_n PRIMARY KEY (snap_id,userid,dbid,queryid)
);
CREATE INDEX ix_snap_stmts_qid ON snap_statements (queryid_md5);
COMMENT ON TABLE snap_statements IS 'Snapshot statement statistics table (fields from pg_stat_statements)';
