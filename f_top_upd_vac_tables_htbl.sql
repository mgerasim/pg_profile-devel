
CREATE OR REPLACE FUNCTION top_upd_vac_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Upd</th><th>Upd(HOT)</th><th>Del</th><th>Vacuum</th><th>AutoVacuum</th><th>Analyze</th><th>AutoAnalyze</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        schemaname,
        relname,
        sum(n_tup_upd)-sum(n_tup_hot_upd) as n_tup_upd,
        sum(n_tup_del) as n_tup_del,
        sum(n_tup_hot_upd) as n_tup_hot_upd,
        sum(vacuum_count) as vacuum_count,
        sum(autovacuum_count) as autovacuum_count,
        sum(analyze_count) as analyze_count,
        sum(autoanalyze_count) as autoanalyze_count
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
    HAVING sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) > 0
    ORDER BY sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_upd,
            r_result.n_tup_hot_upd,
            r_result.n_tup_del,
            r_result.vacuum_count,
            r_result.autovacuum_count,
            r_result.analyze_count,
            r_result.autoanalyze_count
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
