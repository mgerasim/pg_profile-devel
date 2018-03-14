CREATE TABLE bl_snaps (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE RESTRICT,
    bl_id integer REFERENCES baselines (bl_id) ON DELETE CASCADE,
    CONSTRAINT bl_snaps_pk PRIMARY KEY (snap_id,bl_id)
);
CREATE INDEX ix_bl_snaps_blid ON bl_snaps(bl_id);
COMMENT ON TABLE bl_snaps IS 'Snapshots in baselines';
