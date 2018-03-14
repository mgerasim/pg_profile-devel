CREATE OR REPLACE FUNCTION tbl_top_mods_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Top modified tuples table
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>Mods</th><th>%Mod</th><th>Last AA</th><th>Size</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (e_id integer, cnt integer) FOR
    SELECT 
        db_e.datname as dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze as mods,
        n_mod_since_analyze*100/(n_live_tup + n_dead_tup) as mods_pct,
        last_autoanalyze,
        pg_size_pretty(relsize) as relsize
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id)
    WHERE db_e.datname not like 'template_' AND st.snap_id = db_e.snap_id
        -- Min 5 MB in size
        AND relsize > 5 * 1024^2
        AND n_mod_since_analyze > 0
    ORDER BY n_mod_since_analyze*100/(n_live_tup + n_dead_tup) DESC
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
            r_result.mods,
            r_result.mods_pct,
            r_result.last_autoanalyze,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
