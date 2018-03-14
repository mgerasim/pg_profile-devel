CREATE VIEW v_snap_stat_user_functions AS
    SELECT
        snap_id,
        dbid,
        funcid,
        schemaname,
        funcname,
        calls,
        total_time,
        self_time
    FROM ONLY snap_stat_user_functions JOIN funcs_list USING (funcid);
COMMENT ON VIEW v_snap_stat_user_indexes IS 'Reconstructed stats view with function names and schemas';
