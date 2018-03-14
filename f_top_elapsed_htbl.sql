CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Elapsed time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) queries ordered by epapsed time 
    c_elapsed_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(total_time),1) AS total_time
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id)
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.total_time*100/tot.total_time) as total_pct,
        min(st.min_time) as min_time,max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) as shared_blks_hit,
        sum(st.shared_blks_read) as shared_blks_read,
        sum(st.shared_blks_dirtied) as shared_blks_dirtied,
        sum(st.shared_blks_written) as shared_blks_written,
        sum(st.local_blks_hit) as local_blks_hit,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_dirtied) as local_blks_dirtied,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.blk_read_time) as blk_read_time,
        sum(st.blk_write_time) as blk_write_time
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    ORDER BY total_time DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.total_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
