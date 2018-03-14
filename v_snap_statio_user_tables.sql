CREATE VIEW v_snap_statio_user_tables AS
    SELECT
        snap_id,
        dbid,
        relid,
        schemaname,
        relname,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff
    FROM ONLY snap_statio_user_tables JOIN tables_list USING (relid);
COMMENT ON VIEW v_snap_statio_user_tables IS 'Reconstructed stats view with table names and schemas';

