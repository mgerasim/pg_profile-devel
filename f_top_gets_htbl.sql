CREATE OR REPLACE FUNCTION top_gets_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Gets sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by gets
    c_gets CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(shared_blks_hit),1) AS shared_blks_hit,
            GREATEST(sum(shared_blks_read),1) AS shared_blks_read
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
        (sum(st.shared_blks_hit + st.shared_blks_read)*100/min(tot.shared_blks_read + tot.shared_blks_hit)) as total_pct,
        sum(st.shared_blks_hit) * 100 / CASE WHEN (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) = 0 THEN 1
            ELSE (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) END as hit_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.shared_blks_hit) + sum(st.shared_blks_read) > 0
    ORDER BY gets DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by gets
    FOR r_result IN c_gets(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.total_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
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

