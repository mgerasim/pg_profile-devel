CREATE TABLE snap_statio_user_indexes (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    relid oid REFERENCES tables_list(relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    indexrelid oid REFERENCES indexes_list(indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT pk_snap_statio_user_indexes PRIMARY KEY (snap_id,dbid,relid,indexrelid)
);
COMMENT ON TABLE snap_statio_user_indexes IS 'Stats increments for user indexes in all databases by snapshots';
