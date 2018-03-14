CREATE TABLE snap_stat_user_functions (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    funcid oid REFERENCES funcs_list(funcid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    calls bigint,
    total_time double precision,
    self_time double precision,
    CONSTRAINT pk_snap_stat_user_functions PRIMARY KEY (snap_id,dbid,funcid)
);
COMMENT ON TABLE snap_stat_user_functions IS 'Stats increments for user functions in all databases by snapshots';
