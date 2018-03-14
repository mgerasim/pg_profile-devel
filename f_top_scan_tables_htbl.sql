
/* ===== Objects report functions ===== */
CREATE OR REPLACE FUNCTION top_scan_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        schemaname,
        relname,
        sum(seq_scan) as seq_scan,
        sum(seq_tup_read) as seq_tup_read,
        sum(idx_scan) as idx_scan,
        sum(idx_tup_fetch) as idx_tup_fetch,
        sum(n_tup_ins) as n_tup_ins,
        sum(n_tup_upd)-sum(n_tup_hot_upd) as n_tup_upd,
        sum(n_tup_del) as n_tup_del,
        sum(n_tup_hot_upd) as n_tup_hot_upd
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
    HAVING sum(seq_scan) > 0
    ORDER BY sum(seq_scan) DESC
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
            r_result.seq_scan,
            r_result.seq_tup_read,
            r_result.idx_scan,
            r_result.idx_tup_fetch,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
