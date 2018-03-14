/* ===== Functions report ===== */

CREATE OR REPLACE FUNCTION func_top_time_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.funcname,
        sum(st.calls) as calls,
        sum(st.total_time) as total_time,
        sum(st.self_time) as self_time,
        sum(st.total_time)/sum(st.calls) as m_time,
        sum(st.self_time)/sum(st.calls) as m_stime
    FROM v_snap_stat_user_functions st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
    ORDER BY sum(st.total_time) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
