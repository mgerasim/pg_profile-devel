CREATE TABLE snap_stat_user_indexes (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    relid oid REFERENCES tables_list(relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    indexrelid oid REFERENCES indexes_list(indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    relsize bigint,
    relsize_diff bigint,
    indisunique bool,
    CONSTRAINT pk_snap_stat_user_indexes PRIMARY KEY (snap_id,dbid,relid,indexrelid)
);
COMMENT ON TABLE snap_stat_user_indexes IS 'Stats increments for user indexes in all databases by snapshots';

