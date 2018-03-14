CREATE TABLE last_stat_cluster AS SELECT * FROM snap_stat_cluster WHERE 0=1;
COMMENT ON TABLE last_stat_cluster IS 'Last snapshot data for calculating diffs in next snapshot';
