CREATE OR REPLACE FUNCTION snapshot_show(IN days integer = NULL) RETURNS TABLE(snapshot integer, date_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT snap_id, snap_time
    FROM snapshots
    WHERE days IS NULL OR snap_time > now() - (days || ' days')::interval
    ORDER BY snap_id;
$$ LANGUAGE SQL;
