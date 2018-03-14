CREATE VIEW v_snap_statio_user_indexes AS
    SELECT
        snap_id,
        dbid,
        s.relid,
        s.indexrelid,
        il.schemaname,
        tl.relname,
        il.indexrelname,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff
    FROM
        ONLY snap_statio_user_indexes s
        JOIN tables_list tl ON (s.relid = tl.relid)
        JOIN indexes_list il ON (s.indexrelid = il.indexrelid);
COMMENT ON VIEW v_snap_statio_user_indexes IS 'Reconstructed stats view with table and index names and schemas';
