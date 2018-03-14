CREATE OR REPLACE FUNCTION ix_top_io_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>Blk Reads</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.relname,
        st.indexrelname,
        sum(st.idx_blks_read) as idx_blks_read
    FROM v_snap_statio_user_indexes st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,indexrelid,db_s.datname,st.schemaname,st.relname,st.indexrelname
    ORDER BY sum(st.idx_blks_read) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
    report := report||format(
        row_tpl,
        r_result.dbname,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_read
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
