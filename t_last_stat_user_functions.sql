CREATE TABLE last_stat_user_functions AS SELECT * FROM v_snap_stat_user_functions WHERE 0=1;
COMMENT ON TABLE last_stat_user_functions IS 'Last snapshot data for calculating diffs in next snapshot';

