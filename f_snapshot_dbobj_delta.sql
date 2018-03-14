CREATE OR REPLACE FUNCTION snapshot_dbobj_delta(IN s_id integer, IN topn integer) RETURNS integer AS $$
DECLARE
    qres    record;
BEGIN
    -- Collecting stat info for objects of all databases
    PERFORM collect_obj_stats(s_id);

    -- Calculating difference from previous snapshot and storing it in snap_stat_ tables
    -- Stats of user tables
    FOR qres IN
        SELECT 
            snap_id,
            dbid,
            relid,
            schemaname,
            relname,
            seq_scan,
            seq_tup_read,
            idx_scan,
            idx_tup_fetch,
            n_tup_ins,
            n_tup_upd,
            n_tup_del,
            n_tup_hot_upd,
            n_live_tup,
            n_dead_tup,
            n_mod_since_analyze,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count,
            relsize,
            relsize_diff
        FROM
            (SELECT 
                t.snap_id,
                t.dbid,
                t.relid,
                t.schemaname,
                t.relname,
                t.seq_scan-l.seq_scan as seq_scan,
                t.seq_tup_read-l.seq_tup_read as seq_tup_read,
                t.idx_scan-l.idx_scan as idx_scan,
                t.idx_tup_fetch-l.idx_tup_fetch as idx_tup_fetch,
                t.n_tup_ins-l.n_tup_ins as n_tup_ins,
                t.n_tup_upd-l.n_tup_upd as n_tup_upd,
                t.n_tup_del-l.n_tup_del as n_tup_del,
                t.n_tup_hot_upd-l.n_tup_hot_upd as n_tup_hot_upd,
                t.n_live_tup as n_live_tup,
                t.n_dead_tup as n_dead_tup,
                t.n_mod_since_analyze,
                t.last_vacuum,
                t.last_autovacuum,
                t.last_analyze,
                t.last_autoanalyze,
                t.vacuum_count-l.vacuum_count as vacuum_count,
                t.autovacuum_count-l.autovacuum_count as autovacuum_count,
                t.analyze_count-l.analyze_count as analyze_count,
                t.autoanalyze_count-l.autoanalyze_count as autoanalyze_count,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                row_number() OVER (ORDER BY t.seq_scan-l.seq_scan desc) scan_rank,
                row_number() OVER (ORDER BY t.n_tup_ins-l.n_tup_ins+t.n_tup_upd-l.n_tup_upd+t.n_tup_del-l.n_tup_del+t.n_tup_hot_upd-l.n_tup_hot_upd desc) dml_rank,
                row_number() OVER (ORDER BY t.n_tup_upd-l.n_tup_upd+t.n_tup_del-l.n_tup_del+t.n_tup_hot_upd-l.n_tup_hot_upd desc) vacuum_rank,
                row_number() OVER (ORDER BY t.relsize-l.relsize desc) growth_rank,
                row_number() OVER (ORDER BY t.n_dead_tup*100/GREATEST(t.n_live_tup+t.n_dead_tup,1) desc) dead_pct_rank,
                row_number() OVER (ORDER BY t.n_mod_since_analyze*100/GREATEST(t.n_live_tup,1) desc) mod_pct_rank
            FROM temp_stat_user_tables t JOIN last_stat_user_tables l USING (dbid,relid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id) diff
        WHERE scan_rank <= topn OR dml_rank <= topn OR growth_rank <= topn OR dead_pct_rank <= topn OR mod_pct_rank <= topn OR vacuum_rank <= topn
    LOOP
        INSERT INTO tables_list VALUES (qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_tables VALUES (
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.seq_scan,
            qres.seq_tup_read,
            qres.idx_scan,
            qres.idx_tup_fetch,
            qres.n_tup_ins,
            qres.n_tup_upd,
            qres.n_tup_del,
            qres.n_tup_hot_upd,
            qres.n_live_tup,
            qres.n_dead_tup,
            qres.n_mod_since_analyze,
            qres.last_vacuum,
            qres.last_autovacuum,
            qres.last_analyze,
            qres.last_autoanalyze,
            qres.vacuum_count,
            qres.autovacuum_count,
            qres.analyze_count,
            qres.autoanalyze_count,
            qres.relsize,
            qres.relsize_diff
        );
    END LOOP;

    -- Stats of user indexes
    FOR qres IN
        SELECT
            snap_id,
            dbid,
            relid,
            indexrelid,
            schemaname,
            relname,
            indexrelname,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            relsize,
            relsize_diff,
            indisunique
        FROM
            (SELECT 
                t.snap_id,
                t.dbid,
                t.relid,
                t.indexrelid,
                t.schemaname,
                t.relname,
                t.indexrelname,
                t.idx_scan-l.idx_scan as idx_scan,
                t.idx_tup_read-l.idx_tup_read as idx_tup_read,
                t.idx_tup_fetch-l.idx_tup_fetch as idx_tup_fetch,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                t.indisunique,
                row_number() OVER (ORDER BY t.relsize-l.relsize desc) size_rank -- most growing
            FROM temp_stat_user_indexes t JOIN last_stat_user_indexes l USING (dbid,relid,indexrelid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id) diff
        WHERE size_rank <= topn
    LOOP
        INSERT INTO indexes_list VALUES (qres.indexrelid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        INSERT INTO tables_list VALUES (qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_indexes VALUES (
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.indexrelid,
            qres.idx_scan,
            qres.idx_tup_read,
            qres.idx_tup_fetch,
            qres.relsize,
            qres.relsize_diff,
            qres.indisunique
        );
    END LOOP;
    
    -- Stats of growing unused user indexes
    FOR qres IN
        SELECT
            snap_id,
            dbid,
            relid,
            indexrelid,
            schemaname,
            relname,
            indexrelname,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            relsize,
            relsize_diff,
            indisunique
        FROM
            (SELECT 
                t.snap_id,
                t.dbid,
                t.relid,
                t.indexrelid,
                t.schemaname,
                t.relname,
                t.indexrelname,
                t.idx_scan-l.idx_scan as idx_scan,
                t.idx_tup_read-l.idx_tup_read as idx_tup_read,
                t.idx_tup_fetch-l.idx_tup_fetch as idx_tup_fetch,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                t.indisunique,
                row_number() OVER (ORDER BY t.relsize-l.relsize desc) size_rank
            FROM temp_stat_user_indexes t JOIN last_stat_user_indexes l USING (dbid,relid,indexrelid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND
                NOT t.indisunique
                AND t.idx_scan-l.idx_scan = 0) diff
        WHERE size_rank <= topn
    LOOP
        INSERT INTO indexes_list VALUES (qres.indexrelid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        INSERT INTO tables_list VALUES (qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_indexes VALUES (
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.indexrelid,
            qres.idx_scan,
            qres.idx_tup_read,
            qres.idx_tup_fetch,
            qres.relsize,
            qres.relsize_diff,
            qres.indisunique
        ) ON CONFLICT DO NOTHING;
    END LOOP;

    -- User functions stats
    --INSERT INTO snap_stat_user_functions
    FOR qres IN
        SELECT
            snap_id,
            dbid,
            funcid,
            schemaname,
            funcname,
            calls,
            total_time,
            self_time
        FROM
            (SELECT 
                t.snap_id,
                t.dbid,
                t.funcid,
                t.schemaname,
                t.funcname,
                t.calls-l.calls as calls,
                t.total_time-l.total_time as total_time,
                t.self_time-l.self_time as self_time,
                row_number() OVER (ORDER BY t.total_time-l.total_time desc) time_rank,
                row_number() OVER (ORDER BY t.self_time-l.self_time desc) stime_rank,
                row_number() OVER (ORDER BY t.calls-l.calls desc) calls_rank
            FROM temp_stat_user_functions t JOIN last_stat_user_functions l USING (dbid,funcid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id
                AND t.calls-l.calls > 0) diff
        WHERE time_rank <= topn OR calls_rank <= topn OR stime_rank <= topn
    LOOP
        INSERT INTO funcs_list VALUES (qres.funcid,qres.schemaname,qres.funcname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_functions VALUES (
            qres.snap_id,
            qres.dbid,
            qres.funcid,
            qres.calls,
            qres.total_time,
            qres.self_time
        );
    END LOOP;

    --INSERT INTO snap_statio_user_tables
    FOR qres IN
        SELECT
            snap_id,
            dbid,
            relid,
            schemaname,
            relname,
            heap_blks_read,
            heap_blks_hit,
            idx_blks_read,
            idx_blks_hit,
            toast_blks_read,
            toast_blks_hit,
            tidx_blks_read,
            tidx_blks_hit,
            relsize,
            relsize_diff
        FROM
            (SELECT 
                t.snap_id,
                t.dbid,
                t.relid,
                t.schemaname,
                t.relname,
                t.heap_blks_read-l.heap_blks_read as heap_blks_read,
                t.heap_blks_hit-l.heap_blks_hit as heap_blks_hit,
                t.idx_blks_read-l.idx_blks_read as idx_blks_read,
                t.idx_blks_hit-l.idx_blks_hit as idx_blks_hit,
                t.toast_blks_read-l.toast_blks_read as toast_blks_read,
                t.toast_blks_hit-l.toast_blks_hit as toast_blks_hit,
                t.tidx_blks_read-l.tidx_blks_read as tidx_blks_read,
                t.tidx_blks_hit-l.tidx_blks_hit as tidx_blks_hit,
                t.relsize as relsize,
                t.relsize-l.relsize as relsize_diff,
                row_number() OVER (ORDER BY t.heap_blks_read-l.heap_blks_read+
                t.idx_blks_read-l.idx_blks_read+t.toast_blks_read-l.toast_blks_read+
                t.tidx_blks_read-l.tidx_blks_read desc) read_rank
            FROM temp_statio_user_tables t JOIN last_statio_user_tables l USING (dbid,relid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND
                t.heap_blks_read-l.heap_blks_read+
                t.idx_blks_read-l.idx_blks_read+t.toast_blks_read-l.toast_blks_read+
                t.tidx_blks_read-l.tidx_blks_read > 0) diff
        WHERE read_rank <= topn
    LOOP
        INSERT INTO tables_list VALUES (qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statio_user_tables VALUES (
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.heap_blks_read,
            qres.heap_blks_hit,
            qres.idx_blks_read,
            qres.idx_blks_hit,
            qres.toast_blks_read,
            qres.toast_blks_hit,
            qres.tidx_blks_read,
            qres.tidx_blks_hit,
            qres.relsize,
            qres.relsize_diff
        );
    END LOOP;

    --INSERT INTO snap_statio_user_indexes
    FOR qres IN
        SELECT
            snap_id,
            dbid,
            relid,
            indexrelid,
            schemaname,
            relname,
            indexrelname,
            idx_blks_read,
            idx_blks_hit,
            relsize,
            relsize_diff
        FROM
            (SELECT 
                t.snap_id,
                t.dbid,
                t.relid,
                t.indexrelid,
                t.schemaname,
                t.relname,
                t.indexrelname,
                t.idx_blks_read-l.idx_blks_read as idx_blks_read,
                t.idx_blks_hit-l.idx_blks_hit as idx_blks_hit,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                row_number() OVER (ORDER BY t.idx_blks_read-l.idx_blks_read desc) read_rank
            FROM temp_statio_user_indexes t JOIN last_statio_user_indexes l USING (dbid,relid,indexrelid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND
                t.idx_blks_read-l.idx_blks_read > 0) diff
        WHERE read_rank <= topn
    LOOP
        INSERT INTO indexes_list VALUES (qres.indexrelid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        INSERT INTO tables_list VALUES (qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statio_user_indexes VALUES (
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.indexrelid,
            qres.idx_blks_read,
            qres.idx_blks_hit,
            qres.relsize,
            qres.relsize_diff
        );
    END LOOP;

    -- Renew data in last_ tables, holding data for next diff snapshot
    TRUNCATE TABLE last_stat_user_tables;
    INSERT INTO last_stat_user_tables
    SELECT * FROM temp_stat_user_tables;

    TRUNCATE TABLE last_stat_user_indexes;
    INSERT INTO last_stat_user_indexes
    SELECT * FROM temp_stat_user_indexes;

    TRUNCATE TABLE last_stat_user_functions;
    INSERT INTO last_stat_user_functions
    SELECT * FROM temp_stat_user_functions;

    TRUNCATE TABLE last_statio_user_tables;
    INSERT INTO last_statio_user_tables
    SELECT * FROM temp_statio_user_tables;

    TRUNCATE TABLE last_statio_user_indexes;
    INSERT INTO last_statio_user_indexes
    SELECT * FROM temp_statio_user_indexes;
    RETURN 0;
END;
$$ LANGUAGE plpgsql;
