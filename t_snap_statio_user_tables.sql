CREATE TABLE snap_statio_user_tables (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    relid oid REFERENCES tables_list(relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT pk_snap_statio_user_tables PRIMARY KEY (snap_id,dbid,relid)
);
COMMENT ON TABLE snap_statio_user_tables IS 'IO Stats increments for user tables in all databases by snapshots';
