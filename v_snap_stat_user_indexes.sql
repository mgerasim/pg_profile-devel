CREATE VIEW v_snap_stat_user_indexes AS
    SELECT
        snap_id,
        dbid,
        s.relid,
        s.indexrelid,
        il.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        relsize,
        relsize_diff,
        indisunique
    FROM ONLY 
        snap_stat_user_indexes s 
        JOIN indexes_list il ON (il.indexrelid = s.indexrelid)
        JOIN tables_list tl ON (tl.relid = s.relid);
COMMENT ON VIEW v_snap_stat_user_indexes IS 'Reconstructed stats view with table and index names and schemas';
