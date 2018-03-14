CREATE TABLE last_statio_user_indexes AS SELECT * FROM v_snap_statio_user_indexes WHERE 0=1;
COMMENT ON TABLE last_statio_user_indexes IS 'Last snapshot data for calculating diffs in next snapshot';
