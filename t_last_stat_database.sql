CREATE TABLE last_stat_database AS SELECT * FROM snap_stat_database WHERE 0=1;
COMMENT ON TABLE last_stat_database IS 'Last snapshot data for calculating diffs in next snapshot';
