CREATE OR REPLACE FUNCTION cluster_stats_htbl(IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Metric</th><th>Value</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT
        sum(checkpoints_timed) as checkpoints_timed,
        sum(checkpoints_req) as checkpoints_req,
        sum(checkpoint_write_time) as checkpoint_write_time,
        sum(checkpoint_sync_time) as checkpoint_sync_time,
        sum(buffers_checkpoint) as buffers_checkpoint,
        sum(buffers_clean) as buffers_clean,
        sum(buffers_backend) as buffers_backend,
        sum(buffers_backend_fsync) as buffers_backend_fsync,
        sum(maxwritten_clean) as maxwritten_clean,
        sum(buffers_alloc) as buffers_alloc,
        pg_size_pretty(sum(wal_size)) as wal_size
    FROM snap_stat_cluster
    WHERE snap_id between s_id + 1 and e_id
    HAVING max(stats_reset)=min(stats_reset);

    r_result RECORD;
BEGIN
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(row_tpl,'Scheduled checkpoints',r_result.checkpoints_timed);
        report := report||format(row_tpl,'Requested checkpoints',r_result.checkpoints_req);
        report := report||format(row_tpl,'Checkpoint write time (s)',round(cast(r_result.checkpoint_write_time/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoint sync time (s)',round(cast(r_result.checkpoint_sync_time/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoints pages written',r_result.buffers_checkpoint);
        report := report||format(row_tpl,'Background pages written',r_result.buffers_clean);
        report := report||format(row_tpl,'Backend pages written',r_result.buffers_backend);
        report := report||format(row_tpl,'Backend fsync count',r_result.buffers_backend_fsync);
        report := report||format(row_tpl,'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean);
        report := report||format(row_tpl,'Number of buffers allocated',r_result.buffers_alloc);
        report := report||format(row_tpl,'WAL generated',r_result.wal_size);
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
