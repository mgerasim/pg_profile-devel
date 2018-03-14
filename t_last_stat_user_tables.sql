CREATE TABLE last_stat_user_tables AS SELECT * FROM v_snap_stat_user_tables WHERE 0=1;
COMMENT ON TABLE last_stat_user_tables IS 'Last snapshot data for calculating diffs in next snapshot';
