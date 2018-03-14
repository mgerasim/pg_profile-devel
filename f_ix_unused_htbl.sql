CREATE OR REPLACE FUNCTION ix_unused_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>ixSize</th><th>Table DML ops (w/o HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_ix_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_e.datname as dbname,
        schemaname,
        relname,
        indexrelname,
        pg_size_pretty(max(ix_last.relsize)) as relsize,
        sum(tab.n_tup_ins+tab.n_tup_upd+tab.n_tup_del) as dml_ops
    FROM v_snap_stat_user_indexes ix
        JOIN v_snap_stat_user_tables tab USING (snap_id,dbid,relid,schemaname,relname)
        JOIN v_snap_stat_user_indexes ix_last USING (dbid,relid,indexrelid,schemaname,relname,indexrelname)
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=ix.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=ix.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE ix_last.snap_id = db_e.snap_id 
        AND ix.snap_id BETWEEN db_s.snap_id + 1 and db_e.snap_id
        AND NOT ix.indisunique
        AND ix.idx_scan = 0
    GROUP BY dbid,relid,indexrelid,dbname,schemaname,relname,indexrelname
    ORDER BY sum(tab.n_tup_ins+tab.n_tup_upd+tab.n_tup_del) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_ix_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.dml_ops
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
