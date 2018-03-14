CREATE OR REPLACE FUNCTION top_iowait_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- IOWait time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>IO wait(s)</th><th>%Total</th><th>Reads</th><th>Writes</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by I/O Wait time 
    c_iowait_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT 
            CASE WHEN sum(blk_read_time) = 0 THEN 1 ELSE sum(blk_read_time) END AS blk_read_time,
            CASE WHEN sum(blk_write_time) = 0 THEN 1 ELSE sum(blk_write_time) END AS blk_write_time
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
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
        sum(st.blk_write_time) as blk_write_time,
        (sum(st.blk_read_time + st.blk_write_time))/1000 as io_time,
        (sum(st.blk_read_time + st.blk_write_time)*100/min(tot.blk_read_time+tot.blk_write_time)) as total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.blk_read_time) + sum(st.blk_write_time) > 0
    ORDER BY io_time DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.total_pct AS numeric),2),
            round(CAST(r_result.shared_blks_read AS numeric)),
            round(CAST(r_result.shared_blks_written AS numeric)),
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
