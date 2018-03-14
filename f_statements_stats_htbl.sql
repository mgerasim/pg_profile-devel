CREATE OR REPLACE FUNCTION statements_stats_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT 
        db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.shared_blks_hit + st.shared_blks_read) as shared_gets,
        sum(st.local_blks_hit + st.local_blks_read) as local_gets,
        sum(st.shared_blks_dirtied) as shared_blks_dirtied,
        sum(st.local_blks_dirtied) as local_blks_dirtied,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.statements) as statements
    FROM snap_statements_total st 
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id)
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY ROLLUP(db_s.datname)
    ORDER BY db_s.datname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
