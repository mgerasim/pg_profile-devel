CREATE TABLE last_stat_user_indexes AS SELECT * FROM v_snap_stat_user_indexes WHERE 0=1;
COMMENT ON TABLE last_stat_user_indexes IS 'Last snapshot data for calculating diffs in next snapshot';
