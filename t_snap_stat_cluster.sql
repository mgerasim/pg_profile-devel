CREATE TABLE snap_stat_cluster
(
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamp with time zone,
    wal_size bigint,
    CONSTRAINT pk_snap_stat_cluster PRIMARY KEY (snap_id)
);
COMMENT ON TABLE snap_stat_cluster IS 'Snapshot cluster statistics table (fields from pg_stat_bgwriter, etc.)';
