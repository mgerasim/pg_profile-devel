CREATE OR REPLACE FUNCTION top_temp_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Temp usage sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Work_w(blk)</th><th>%Total</th><th>Work_r(blk)</th><th>%Total</th><th>Local_w(blk)</th><th>%Total</th><th>Local_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by temp usage 
    c_temp CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT
            GREATEST(sum(temp_blks_read),1) AS temp_blks_read,
            GREATEST(sum(temp_blks_written),1) AS temp_blks_written,
            GREATEST(sum(local_blks_read),1) AS local_blks_read,
            GREATEST(sum(local_blks_written),1) AS local_blks_written
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
        sum(st.shared_blks_hit) * 100 / GREATEST(sum(st.shared_blks_hit)+sum(st.shared_blks_read),1) as hit_pct,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read*100/tot.temp_blks_read) as temp_read_total_pct,
        sum(st.temp_blks_written*100/tot.temp_blks_written) as temp_write_total_pct,
        sum(st.local_blks_read*100/tot.local_blks_read) as local_read_total_pct,
        sum(st.local_blks_written*100/tot.local_blks_written) as local_write_total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written) > 0
    ORDER BY sum(st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
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

CREATE OR REPLACE FUNCTION collect_queries(IN query_id char(10), IN query_text text) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    INSERT INTO queries_list
    VALUES (query_id,regexp_replace(query_text,'\s+',' ','g'))
    ON CONFLICT DO NOTHING;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;
