CREATE TABLE snap_params (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    p_name text,
    setting text,
    CONSTRAINT pk_snap_params PRIMARY KEY (snap_id,p_name)
);
COMMENT ON TABLE snap_params IS 'PostgreSQL parameters at time of snapshot';
