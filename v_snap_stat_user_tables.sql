
CREATE VIEW v_snap_stat_user_tables AS
    SELECT
        snap_id,
        dbid,
        relid,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        relsize,
        relsize_diff
    FROM ONLY snap_stat_user_tables JOIN tables_list USING (relid);
COMMENT ON VIEW v_snap_stat_user_tables IS 'Reconstructed stats view with table names and schemas';
