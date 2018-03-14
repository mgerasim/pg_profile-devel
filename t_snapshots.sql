/* ========= Tables ========= */
CREATE TABLE snapshots (
    snap_id SERIAL PRIMARY KEY,
    snap_time timestamp (0) with time zone
);

CREATE INDEX ix_snap_time ON snapshots(snap_time);
COMMENT ON TABLE snapshots IS 'Snapshot times list';
