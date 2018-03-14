CREATE OR REPLACE FUNCTION tbl_top_dead_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Top dead tuples table
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>%Dead</th><th>Last AV</th><th>Size</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (e_id integer, cnt integer) FOR
    SELECT 
        db_e.datname as dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_dead_tup*100/(n_live_tup + n_dead_tup) as dead_pct,
        last_autovacuum,
        pg_size_pretty(relsize) as relsize
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id)
    WHERE db_e.datname not like 'template_' AND st.snap_id = db_e.snap_id
        -- Min 5 MB in size
        AND st.relsize > 5 * 1024^2
        AND st.n_dead_tup > 0
    ORDER BY n_dead_tup*100/(n_live_tup + n_dead_tup) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.dead_pct,
            r_result.last_autovacuum,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
