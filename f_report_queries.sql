CREATE OR REPLACE FUNCTION report_queries() RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    c_queries CURSOR FOR SELECT queryid, querytext FROM queries_list;
    qr_result RECORD;
    report text := '';
    query_text text := '';
    tab_tpl CONSTANT text := '<table><tr><th>QueryID</th><th>Query Text</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a NAME=%s>%s</a></td><td>%s</td></tr>';
BEGIN
    FOR qr_result IN c_queries LOOP
        query_text := replace(qr_result.querytext,'<','&lt;');
        query_text := replace(query_text,'>','&gt;');
        report := report||format(
            row_tpl,
            qr_result.queryid,
            qr_result.queryid,
            query_text
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

