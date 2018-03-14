/* ========= Reporting functions ========= */

/* ===== Cluster report functions ===== */
CREATE OR REPLACE FUNCTION dbstats_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Commits</th><th>Rollbacks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Growth</th><th>Deadlocks</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT 
        datname as dbname,
        sum(xact_commit) as xact_commit,
        sum(xact_rollback) as xact_rollback,
        sum(blks_read) as blks_read,
        sum(blks_hit) as blks_hit,
        sum(tup_returned) as tup_returned,
        sum(tup_fetched) as tup_fetched,
        sum(tup_inserted) as tup_inserted,
        sum(tup_updated) as tup_updated,
        sum(tup_deleted) as tup_deleted,
        sum(temp_files) as temp_files,
        pg_size_pretty(sum(temp_bytes)) as temp_bytes,
        pg_size_pretty(sum(datsize_delta)) as datsize_delta,
        sum(deadlocks) as deadlocks, 
        sum(blks_hit)*100/GREATEST(sum(blks_hit)+sum(blks_read),1) as blks_hit_pct
    FROM snap_stat_database
    WHERE datname not like 'template_' and snap_id between s_id + 1 and e_id
    GROUP BY datid,datname
    HAVING max(stats_reset)=min(stats_reset);

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize_delta,
            r_result.deadlocks
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
