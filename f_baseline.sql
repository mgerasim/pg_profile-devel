/* ========= Baseline management functions ========= */

CREATE OR REPLACE FUNCTION baseline_new(IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    baseline_id integer;
BEGIN
    INSERT INTO baselines(bl_name,keep_until)
    VALUES (name,now() + (days || ' days')::interval)
    RETURNING bl_id INTO baseline_id;

    INSERT INTO bl_snaps (snap_id,bl_id)
    SELECT snap_id, baseline_id
    FROM snapshots
    WHERE snap_id BETWEEN start_id AND end_id;

    RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_drop(IN name varchar(25) = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM baselines WHERE name IS NULL OR bl_name = name;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_keep(IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE name IS NULL OR bl_name = name;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_show() RETURNS TABLE(baseline varchar(25), min_snap integer, max_snap integer, keep_until_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT bl_name as baseline,min_snap_id,max_snap_id, keep_until 
    FROM baselines b JOIN 
        (SELECT bl_id,min(snap_id) min_snap_id,max(snap_id) max_snap_id FROM bl_snaps GROUP BY bl_id) b_agg
    USING (bl_id)
    ORDER BY min_snap_id;
$$ LANGUAGE SQL;
