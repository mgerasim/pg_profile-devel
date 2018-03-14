CREATE OR REPLACE FUNCTION top_growth_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.relname,
        sum(st.seq_scan) as seq_scan,
        sum(st.seq_tup_read) as seq_tup_read,
        sum(st.idx_scan) as idx_scan,
        sum(st.idx_tup_fetch) as idx_tup_fetch,
        sum(st.n_tup_ins) as n_tup_ins,
        sum(st.n_tup_upd)-sum(st.n_tup_hot_upd) as n_tup_upd,
        sum(st.n_tup_del) as n_tup_del,
        sum(st.n_tup_hot_upd) as n_tup_hot_upd,
        pg_size_pretty(sum(st.relsize_diff)) as growth,
        pg_size_pretty(max(st_last.relsize)) as relsize
    FROM v_snap_stat_user_tables st
        JOIN v_snap_stat_user_tables st_last using (dbid,relid)
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
        AND st_last.snap_id=db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,st.schemaname,st.relname
    HAVING sum(st.relsize_diff) > 0
    ORDER BY sum(st.relsize_diff) DESC
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
            r_result.relsize,
            r_result.growth,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
