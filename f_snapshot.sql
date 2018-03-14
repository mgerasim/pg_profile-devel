CREATE OR REPLACE FUNCTION snapshot() RETURNS integer SET search_path=@extschema@,public SET lock_timeout=300000 AS $$
DECLARE
    id    integer;
    topn  integer;
    ret   integer;
    lockid      bigint;
    b_local_db  boolean;
    pg_version  varchar(10);
    qres        record;
BEGIN
    -- Only one running snapshot() function allowed!
    -- Getting custom lockid
    BEGIN
        lockid := current_setting('pg_profile.lockid')::bigint;
    EXCEPTION
        WHEN OTHERS THEN lockid := 2174049485089987259;
    END;
    IF NOT pg_try_advisory_lock(lockid) THEN
        RAISE 'Another snapshot() function is running!';
    END IF;
    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;
    -- Getting retention setting
    BEGIN
        ret := current_setting('pg_profile.retention')::integer;
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;
    
    --Getting postgres version
    SELECT setting INTO STRICT pg_version FROM pg_catalog.pg_settings WHERE name = 'server_version_num';

    -- Deleting obsolete baselines
    DELETE FROM baselines WHERE keep_until < now();
    -- Deleting obsolote snapshots
    DELETE FROM snapshots WHERE snap_time < now() - (ret || ' days')::interval
        AND snap_id NOT IN (SELECT snap_id FROM bl_snaps);

    -- Creating a new snapshot record
    INSERT INTO snapshots(snap_time) 
    VALUES (now())
    RETURNING snap_id INTO id;

    -- Collecting postgres parameters
    INSERT INTO snap_params
    SELECT id,name,setting
    FROM pg_catalog.pg_settings
    WHERE name IN ('pg_stat_statements.max','pg_stat_statements.track');
    
    INSERT INTO snap_params
    VALUES (id,'pg_profile.topn',topn);

    -- Snapshot data from pg_stat_statements for top whole cluster statements
    FOR qres IN
        SELECT
            id,
            st.userid,
            st.dbid,
            st.queryid,
            left(md5(db.datname || r.rolname || st.query ), 10) AS queryid_md5,
            st.calls,
            st.total_time,
            st.min_time,
            st.max_time,
            st.mean_time,
            st.stddev_time,
            st.rows,
            st.shared_blks_hit,
            st.shared_blks_read,
            st.shared_blks_dirtied,
            st.shared_blks_written,
            st.local_blks_hit,
            st.local_blks_read,
            st.local_blks_dirtied,
            st.local_blks_written,
            st.temp_blks_read,
            st.temp_blks_written,
            st.blk_read_time,
            st.blk_write_time,
            regexp_replace(st.query,'\s+',' ','g') AS query
        FROM pg_stat_statements st 
            JOIN pg_database db ON (db.oid=st.dbid)
            JOIN pg_roles r ON (r.oid=st.userid)
        JOIN
            (SELECT
            userid, dbid, md5(query) as q_md5,
            row_number() over (ORDER BY sum(total_time) DESC) AS time_p, 
            row_number() over (ORDER BY sum(calls) DESC) AS calls_p,
            row_number() over (ORDER BY sum(blk_read_time + blk_write_time) DESC) AS io_time_p,
            row_number() over (ORDER BY sum(shared_blks_hit + shared_blks_read) DESC) AS gets_p,
            row_number() over (ORDER BY sum(temp_blks_written + local_blks_written) DESC) AS temp_p
            FROM pg_stat_statements
            GROUP BY userid, dbid, md5(query)) rank_t
        ON (st.userid=rank_t.userid AND st.dbid=rank_t.dbid AND md5(st.query)=rank_t.q_md5)
        WHERE
            time_p <= topn 
            OR calls_p <= topn
            OR io_time_p <= topn
            OR gets_p <= topn
            OR temp_p <= topn
    LOOP
        INSERT INTO stmt_list VALUES (qres.queryid_md5,qres.query) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statements VALUES (
            qres.id,
            qres.userid,
            qres.dbid,
            qres.queryid,
            qres.queryid_md5,
            qres.calls,
            qres.total_time,
            qres.min_time,
            qres.max_time,
            qres.mean_time,
            qres.stddev_time,
            qres.rows,
            qres.shared_blks_hit,
            qres.shared_blks_read,
            qres.shared_blks_dirtied,
            qres.shared_blks_written,
            qres.local_blks_hit,
            qres.local_blks_read,
            qres.local_blks_dirtied,
            qres.local_blks_written,
            qres.temp_blks_read,
            qres.temp_blks_written,
            qres.blk_read_time,
            qres.blk_write_time
        );
    END LOOP;

    -- Deleting unused statements
    DELETE FROM stmt_list
    WHERE queryid_md5 NOT IN
    (SELECT queryid_md5 FROM snap_statements);

    -- Aggregeted statistics data
    INSERT INTO snap_statements_total
    SELECT id,dbid,sum(calls),sum(total_time),sum(rows),sum(shared_blks_hit),sum(shared_blks_read),sum(shared_blks_dirtied),sum(shared_blks_written),
        sum(local_blks_hit),sum(local_blks_read),sum(local_blks_dirtied),sum(local_blks_written),sum(temp_blks_read),sum(temp_blks_written),sum(blk_read_time),
        sum(blk_write_time),count(*)
    FROM pg_stat_statements
    GROUP BY dbid;
    -- Flushing pg_stat_statements
    PERFORM pg_stat_statements_reset();

    -- pg_stat_database data
    INSERT INTO snap_stat_database 
    SELECT 
        id,
        rs.datid,
        rs.datname,
        rs.xact_commit-ls.xact_commit,
        rs.xact_rollback-ls.xact_rollback,
        rs.blks_read-ls.blks_read,
        rs.blks_hit-ls.blks_hit,
        rs.tup_returned-ls.tup_returned,
        rs.tup_fetched-ls.tup_fetched,
        rs.tup_inserted-ls.tup_inserted,
        rs.tup_updated-ls.tup_updated,
        rs.tup_deleted-ls.tup_deleted,
        rs.conflicts-ls.conflicts,
        rs.temp_files-ls.temp_files,
        rs.temp_bytes-ls.temp_bytes,
        rs.deadlocks-ls.deadlocks,
        rs.blk_read_time-ls.blk_read_time,
        rs.blk_write_time-ls.blk_write_time,
        rs.stats_reset,
        pg_database_size(rs.datid)-ls.datsize_delta
    FROM pg_catalog.pg_stat_database rs 
    JOIN ONLY(last_stat_database) ls ON (rs.datid = ls.datid AND rs.datname = ls.datname AND rs.stats_reset = ls.stats_reset AND ls.snap_id = id - 1);

    PERFORM snapshot_dbobj_delta(id,topn);
    
    TRUNCATE TABLE last_stat_database;

    INSERT INTO last_stat_database (
        snap_id,
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        blk_read_time,
        blk_write_time,
        stats_reset,
        datsize_delta)
    SELECT 
        id,
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        blk_read_time,
        blk_write_time,
        stats_reset,
        pg_database_size(datid)
    FROM pg_catalog.pg_stat_database;
    
    -- pg_stat_bgwriter data
    IF pg_version::integer < 100000 THEN
        INSERT INTO snap_stat_cluster
        SELECT 
            id,
            rs.checkpoints_timed-ls.checkpoints_timed,
            rs.checkpoints_req-ls.checkpoints_req,
            rs.checkpoint_write_time-ls.checkpoint_write_time,
            rs.checkpoint_sync_time-ls.checkpoint_sync_time,
            rs.buffers_checkpoint-ls.buffers_checkpoint,
            rs.buffers_clean-ls.buffers_clean,
            rs.maxwritten_clean-ls.maxwritten_clean,
            rs.buffers_backend-ls.buffers_backend,
            rs.buffers_backend_fsync-ls.buffers_backend_fsync,
            rs.buffers_alloc-ls.buffers_alloc,
            rs.stats_reset,
            pg_xlog_location_diff(pg_current_xlog_location(),'0/00000000')-ls.wal_size
        FROM pg_catalog.pg_stat_bgwriter rs 
        JOIN ONLY(last_stat_cluster) ls ON (rs.stats_reset = ls.stats_reset AND ls.snap_id = id - 1);
    ELSIF pg_version::integer >= 100000 THEN
        INSERT INTO snap_stat_cluster
        SELECT 
            id,
            rs.checkpoints_timed-ls.checkpoints_timed,
            rs.checkpoints_req-ls.checkpoints_req,
            rs.checkpoint_write_time-ls.checkpoint_write_time,
            rs.checkpoint_sync_time-ls.checkpoint_sync_time,
            rs.buffers_checkpoint-ls.buffers_checkpoint,
            rs.buffers_clean-ls.buffers_clean,
            rs.maxwritten_clean-ls.maxwritten_clean,
            rs.buffers_backend-ls.buffers_backend,
            rs.buffers_backend_fsync-ls.buffers_backend_fsync,
            rs.buffers_alloc-ls.buffers_alloc,
            rs.stats_reset,
            pg_wal_lsn_diff(pg_current_wal_lsn(),'0/00000000')-ls.wal_size
        FROM pg_catalog.pg_stat_bgwriter rs 
        JOIN ONLY(last_stat_cluster) ls ON (rs.stats_reset = ls.stats_reset AND ls.snap_id = id - 1);
    END IF;
    
    TRUNCATE TABLE last_stat_cluster;

    IF pg_version::integer < 100000 THEN
        INSERT INTO last_stat_cluster (
            snap_id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            wal_size)
        SELECT 
            id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            pg_xlog_location_diff(pg_current_xlog_location(),'0/00000000')
        FROM pg_catalog.pg_stat_bgwriter;
    ELSIF pg_version::integer >= 100000 THEN
        INSERT INTO last_stat_cluster (
            snap_id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            wal_size)
        SELECT 
            id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            pg_wal_lsn_diff(pg_current_wal_lsn(),'0/00000000')
        FROM pg_catalog.pg_stat_bgwriter;
    END IF;
    
    -- Delete unused tables from tables list
    DELETE FROM tables_list WHERE relid NOT IN (
        SELECT relid FROM snap_stat_user_tables
        UNION ALL
        SELECT relid FROM snap_statio_user_tables
        UNION ALL
        SELECT relid FROM snap_stat_user_indexes
        UNION ALL
        SELECT relid FROM snap_statio_user_indexes
    );
    
    -- Delete unused indexes from indexes list
    DELETE FROM indexes_list WHERE indexrelid NOT IN (
        SELECT indexrelid FROM snap_stat_user_indexes
        UNION ALL
        SELECT indexrelid FROM snap_statio_user_indexes
    );
    
    -- Delete unused functions from functions list
    DELETE FROM funcs_list WHERE funcid NOT IN (
        SELECT funcid FROM snap_stat_user_functions
    );

    PERFORM pg_advisory_unlock(lockid);
    
    RETURN id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION snapshot() IS 'Statistics snapshot creation function. Must be explicitly called periodically.';
