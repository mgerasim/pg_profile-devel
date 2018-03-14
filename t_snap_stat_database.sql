CREATE TABLE snap_stat_database
(
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    datid oid,
    datname name,
    xact_commit bigint,
    xact_rollback bigint,
    blks_read bigint,
    blks_hit bigint,
    tup_returned bigint,
    tup_fetched bigint,
    tup_inserted bigint,
    tup_updated bigint,
    tup_deleted bigint,
    conflicts bigint,
    temp_files bigint,
    temp_bytes bigint,
    deadlocks bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    stats_reset timestamp with time zone,
    datsize_delta bigint,
    CONSTRAINT pk_snap_stat_database PRIMARY KEY (snap_id,datid,datname)
);
COMMENT ON TABLE snap_stat_database IS 'Snapshot database statistics table (fields from pg_stat_database)';