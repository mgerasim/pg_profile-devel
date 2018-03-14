CREATE OR REPLACE FUNCTION report(IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile report {snaps}</title></head><body><H1>Postgres profile report {snaps}</H1><p>Report interval: {report_start} - {report_end}</p>{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} table tr:nth-child(even) {background-color: #eee;} table tr:nth-child(odd) {background-color: #fff;} table tr:hover{background-color:#d9ffcc} table th {color: black; background-color: #ffcc99;}';
    --Cursor and variable for checking existance of snapshots
    c_snap CURSOR (snapshot_id integer) FOR SELECT * FROM snapshots WHERE snap_id = snapshot_id;
    snap_rec snapshots%rowtype;
BEGIN
    -- Creating temporary table for reported queries
    CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (queryid char(10) PRIMARY KEY, querytext text) ON COMMIT DELETE ROWS;

    -- CSS
    report := replace(report_tpl,'{css}',report_css);

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Checking snapshot existance, header generation
    OPEN c_snap(start_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        report := replace(report,'{report_start}',cast(snap_rec.snap_time as text));
        tmp_text := '(StartID: ' || snap_rec.snap_id ||', ';
    CLOSE c_snap;

    OPEN c_snap(end_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        report := replace(report,'{report_end}',cast(snap_rec.snap_time as text));
        tmp_text := tmp_text || 'EndID: ' || snap_rec.snap_id ||')';
    CLOSE c_snap;
    report := replace(report,'{snaps}',tmp_text);
    tmp_text := '';

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(start_id, end_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>This interval contains snapshots with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- Table of Contents
    tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
    tmp_text := tmp_text || '<li><a HREF=#cl_stat>Cluster statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#db_stat>Databases stats</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#st_stat>Statements stats by database</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#clu_stat>Cluster stats</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL Query stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by executions</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_gets>Top SQL by gets</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_temp>Top SQL by temp usage</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete List of SQL Text</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema objects stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Most scanned tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#vac_tbl>Top Delete/Update tables with vacuum run count</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growing tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_idx>Top growing indexes</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#io_stat>I/O Schema objects stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#tbl_io_stat>Top tables by I/O</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_io_stat>Top indexes by I/O</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#func_stat>User function stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#funs_time_stat>Top functions by total time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#funs_calls_stat>Top functions by executions</a></li>';
    tmp_text := tmp_text || '</ul>';


    tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum related stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#dead_tbl>Tables ordered by dead tuples ratio</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#mod_tbl>Tables ordered by modified tuples ratio</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Cluster statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Databases stats</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(dbstats_htbl(start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=st_stat>Statements stats by database</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(start_id, end_id, topn));
    
    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster stats</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_htbl(start_id, end_id));

    --Reporting on top queries by elapsed time
    tmp_text := tmp_text||'<H2><a NAME=sql_stat>SQL Query stats</a></H2>';
    tmp_text := tmp_text||'<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_elapsed_htbl(start_id, end_id, topn));

    -- Reporting on top queries by executions
    tmp_text := tmp_text||'<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_exec_htbl(start_id, end_id, topn));

    -- Reporting on top queries by I/O wait time
    tmp_text := tmp_text||'<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(start_id, end_id, topn));

    -- Reporting on top queries by gets
    tmp_text := tmp_text||'<H3><a NAME=top_gets>Top SQL by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_gets_htbl(start_id, end_id, topn));

    -- Reporting on top queries by temp usage
    tmp_text := tmp_text||'<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_temp_htbl(start_id, end_id, topn));

    -- Listing queries
    tmp_text := tmp_text||'<H3><a NAME=sql_list>Complete List of SQL Text</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(report_queries());

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text||'<H2><a NAME=schema_stat>Schema objects stats</a></H2>';
    tmp_text := tmp_text||'<H3><a NAME=scanned_tbl>Most seq. scanned tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(start_id, end_id, topn));
    
    tmp_text := tmp_text||'<H3><a NAME=vac_tbl>Top Delete/Update tables with vacuum run count</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_htbl(start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_htbl(start_id, end_id, topn));
    tmp_text := tmp_text||'<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_htbl(start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=ix_unused>Unused growing indexes</a></H3>';
    tmp_text := tmp_text||'<p>This table contains not-scanned indexes (during report period), ordered by number of DML operations on underlying tables. Constraint indexes are excluded.</p>';
    tmp_text := tmp_text || nodata_wrapper(ix_unused_htbl(start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=io_stat>I/O Schema objects stats</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=tbl_io_stat>Top tables by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_io_stat>Top indexes by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_htbl(start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=func_stat>User function stats</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=funs_time_stat>Top functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=funs_calls_stat>Top functions by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(start_id, end_id, topn));

    -- Reporting vacuum related stats
    tmp_text := tmp_text||'<H2><a NAME=vacuum_stats>Vacuum related stats</a></H2>';
    tmp_text := tmp_text||'<p>Data in this section is not incremental. This data is valid for ending snapshot only.</p>';
    tmp_text := tmp_text||'<H3><a NAME=dead_tbl>Tables ordered by dead tuples ratio</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_dead_htbl(start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=mod_tbl>Tables ordered by modified tuples ratio</a></H3>';
    tmp_text := tmp_text||'<p>Table shows modified tuples stats since last analyze.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_mods_htbl(start_id, end_id, topn));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt();
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Snapshot repository contains snapshots with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION report(IN start_id integer, IN end_id integer) IS 'Statistics report generation function. Takes IDs of start and end snapshot (inclusive)';
