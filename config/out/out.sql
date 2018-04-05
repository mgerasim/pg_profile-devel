CREATE TABLE clusters (
    cluster_id SERIAL PRIMARY KEY,
    name varchar(255) NOT NULL,
    connect_username varchar (25) NOT NULL,
    connect_password varchar (25) NOT NULL,
    connect_host varchar (25) NOT NULL,
    connect_port integer NOT NULL DEFAULT 5432,
    connect_database varchar (25) NOT NULL DEFAULT 'postgres',
    is_enabled boolean NOT NULL DEFAULT true,
    CONSTRAINT cns_cluster_host_port UNIQUE(connect_host, connect_port)
);

CREATE UNIQUE INDEX inx_cluster_name on clusters (lower(name));

COMMENT ON TABLE clusters IS 'The list of clusters of databases on which removal of snapshot is executed';-- Function CLUSTER_ADD - adding of a cluster of databases
-- INPUT PARAMETERS:
-- _name                -   unique name cluster of databases
-- _connect_username    -   parameter of connection to a cluster - user name
-- _connect_password    -   parameter of connection to a cluster - password
-- _connect_host        -   parameter of connection to a cluster - host
-- _connect_port        -   parameter of connection to a cluster - port, default 5432
-- _connect_database    -   parameter of connection to a cluster - database, default postgres
-- OUTPUT PARAMETERS:
-- ID - identifier of a cluster of databases
CREATE OR REPLACE FUNCTION cluster_add(IN _name varchar, 
                                       IN _connect_username varchar, 
                                       IN _connect_password varchar,
                                       IN _connect_host varchar, 
                                       IN _connect_port integer DEFAULT 5432, 
                                       IN _connect_database varchar DEFAULT 'postgres') RETURNS bigint 
SET search_path=public
AS $$
DECLARE
    ID    bigint;
BEGIN
    INSERT INTO clusters(name, 
                         connect_username, 
                         connect_password,
                         connect_host, 
                         connect_port, 
                         connect_database) 
    VALUES (_name, 
            _connect_username, 
            _connect_password,
            _connect_host, 
            _connect_port, 
            _connect_database) 
    RETURNING cluster_id INTO ID;            
    RETURN ID;                
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add(IN _name varchar, 
                                IN _connect_username varchar, 
                                IN _connect_password varchar,
                                IN _connect_host varchar, 
                                IN _connect_port integer, 
                                IN _connect_database varchar) IS 'Adding of a cluster of databases';

-- Function CLUSTER_EDIT - updating fields of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:                    
-- _name                    -   name of cluster of databases 
-- _new_connect_username    -   username of cluster of databases for updating
-- _new_connect_password    -   password of cluster of databases for updating
-- _new_connect_host        -   host of cluster of databases for updating
-- _new_connect_port        -   port of cluster of databases for updating
-- _new_connect_database    -   database of cluster of databases for updating
CREATE OR REPLACE FUNCTION cluster_edit(IN _name varchar,
                                        IN _new_connect_username varchar,
                                        IN _new_connect_password varchar, 
                                        IN _new_connect_host varchar, 
                                        IN _new_connect_port integer DEFAULT 5432,
                                        IN _new_connect_database varchar DEFAULT 'postgres') RETURNS void
SET search_path=public
AS $$
DECLARE
    count_updtated integer;
BEGIN
    UPDATE clusters 
        SET connect_username = _new_connect_username,
            connect_password = _new_connect_password,
            connect_host = _new_connect_host,
            connect_port = _new_connect_port,
            connect_database = _new_connect_database
    WHERE name = _name;
    GET DIAGNOSTICS count_updtated = ROW_COUNT;
    IF (count_updtated = 1) THEN
        RAISE NOTICE 'Cluster of databases with name=% is updated', _name;
    ELSE
        RAISE 'Cluster of databases % not found for updating', _name
            USING HINT = 'Check the input parameter _ID for function cluster_edit';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_edit(IN _name varchar,
                                 IN _new_connect_username varchar,
                                 IN _new_connect_password varchar, 
                                 IN _new_connect_host varchar, 
                                 IN _new_connect_port integer,
                                 IN _new_connect_database varchar) IS 'Updating fields of a cluster of databases';-- Function CLUSTER_REMOVE - removing of a cluster of databases
-- INPUT PARAMETERS:
-- _name                -   name cluster of databases for removing, if not found then exception with code P0001
-- OUTPUT PARAMETERS:
-- ID - identifier of a cluster of databases
CREATE OR REPLACE FUNCTION cluster_remove(IN _name varchar) RETURNS void 
SET search_path=public
AS $$
DECLARE
    is_deleted integer;
BEGIN
    DELETE FROM clusters WHERE name = _name;
    GET DIAGNOSTICS is_deleted = ROW_COUNT;
    IF (is_deleted = 1) THEN
        RAISE NOTICE 'Cluster % is removed', _name;
    ELSE
        RAISE 'Cluster % not found for removing', _name
            USING HINT = 'Check the input parameter _name for function cluster_remove(_name)';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_remove(IN _name varchar) IS 'Removing of a cluster of databases';

-- Function CLUSTER_ENABLE - enable create snapshot of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:
-- _name                       -   name of cluster of databases
CREATE OR REPLACE FUNCTION cluster_enable(IN _name varchar) RETURNS void
SET search_path=public
AS $$
DECLARE
    count_updated integer;
BEGIN
    UPDATE clusters 
        SET is_enabled = true
    WHERE name = _name;
    GET DIAGNOSTICS count_updated = ROW_COUNT;
    IF (count_updated = 1) THEN
        RAISE NOTICE 'Cluster of databases with name=% is enabled', _name;
    ELSE
        RAISE 'Cluster of databases % not found for enable', _name
            USING HINT = 'Check the input parameter _name for function cluster_enable';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_enable(IN _ID bigint) IS 'Enable create snapshot of a cluster of databases';

-- Function CLUSTER_ENABLE - enable create snapshot of a all clusters of databases
CREATE OR REPLACE FUNCTION cluster_enable() RETURNS void
SET search_path=public
AS $$
DECLARE
BEGIN
    UPDATE clusters 
        SET is_enabled = true;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_enable(IN _ID bigint) IS 'Enable create snapshot of a all clusters of databases';-- Function CLUSTER_DISABLE - disable create snapshot of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:
-- name                       -   name of cluster of databases
CREATE OR REPLACE FUNCTION cluster_disable(IN _name varchar) RETURNS void
SET search_path=public
AS $$
DECLARE
    count_updated integer;
BEGIN
    UPDATE clusters 
        SET is_enabled = false
    WHERE name = _name;
    GET DIAGNOSTICS count_updated = ROW_COUNT;
    IF (count_updated = 1) THEN
        RAISE NOTICE 'Cluster of databases with name=% is disabled', _name;
    ELSE
        RAISE 'Cluster of databases % not found for disable', _name
            USING HINT = 'Check the input parameter _name for function cluster_disable';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_disable(IN _ID bigint) IS 'Disable create snapshot of a cluster of databases';

-- Function CLUSTER_DISABLE - disable create snapshot of a all clusters of databases
CREATE OR REPLACE FUNCTION cluster_disable() RETURNS void
SET search_path=public
AS $$
DECLARE
BEGIN
    UPDATE clusters 
        SET is_enabled = false;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_disable() IS 'Disable create snapshot of a all clusters of databases';/* ========= Tables ========= */
CREATE TABLE snapshots (
    snap_id SERIAL PRIMARY KEY,
    snap_time timestamp (0) with time zone
);

CREATE INDEX ix_snap_time ON snapshots(snap_time);
COMMENT ON TABLE snapshots IS 'Snapshot times list';
CREATE TABLE snap_params (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    p_name text,
    setting text,
    CONSTRAINT pk_snap_params PRIMARY KEY (snap_id,p_name)
);
COMMENT ON TABLE snap_params IS 'PostgreSQL parameters at time of snapshot';
CREATE TABLE baselines (
    bl_id SERIAL PRIMARY KEY,
    bl_name varchar (25) UNIQUE,
    keep_until timestamp (0) with time zone
);
COMMENT ON TABLE baselines IS 'Baselines list';
CREATE TABLE bl_snaps (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE RESTRICT,
    bl_id integer REFERENCES baselines (bl_id) ON DELETE CASCADE,
    CONSTRAINT bl_snaps_pk PRIMARY KEY (snap_id,bl_id)
);
CREATE INDEX ix_bl_snaps_blid ON bl_snaps(bl_id);
COMMENT ON TABLE bl_snaps IS 'Snapshots in baselines';
CREATE TABLE stmt_list(
    queryid_md5    char(10),
    query          text,
    CONSTRAINT pk_snap_users PRIMARY KEY (queryid_md5)
);
COMMENT ON TABLE stmt_list IS 'Statements, captured in snapshots';
CREATE TABLE snap_statements (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    userid oid,
    dbid oid,
    queryid bigint,
    queryid_md5 char(10) REFERENCES stmt_list (queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    calls bigint,
    total_time double precision,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    stddev_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    CONSTRAINT pk_snap_statements_n PRIMARY KEY (snap_id,userid,dbid,queryid)
);
CREATE INDEX ix_snap_stmts_qid ON snap_statements (queryid_md5);
COMMENT ON TABLE snap_statements IS 'Snapshot statement statistics table (fields from pg_stat_statements)';
CREATE VIEW v_snap_statements AS
SELECT
    st.snap_id as snap_id,
    st.userid as userid,
    st.dbid as dbid,
    st.queryid as queryid,
    queryid_md5 as queryid_md5,
    st.calls as calls,
    st.total_time as total_time,
    st.min_time as min_time,
    st.max_time as max_time,
    st.mean_time as mean_time,
    st.stddev_time as stddev_time,
    st.rows as rows,
    st.shared_blks_hit as shared_blks_hit,
    st.shared_blks_read as shared_blks_read,
    st.shared_blks_dirtied as shared_blks_dirtied,
    st.shared_blks_written as shared_blks_written,
    st.local_blks_hit as local_blks_hit,
    st.local_blks_read as local_blks_read,
    st.local_blks_dirtied as local_blks_dirtied,
    st.local_blks_written as local_blks_written,
    st.temp_blks_read as temp_blks_read,
    st.temp_blks_written as temp_blks_written,
    st.blk_read_time as blk_read_time,
    st.blk_write_time as blk_write_time,
    l.query as query
FROM
    snap_statements st
    JOIN stmt_list l USING (queryid_md5);
CREATE TABLE snap_statements_total (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    calls bigint,
    total_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    statements bigint,
    CONSTRAINT pk_snap_statements_total PRIMARY KEY (snap_id,dbid)
);
COMMENT ON TABLE snap_statements_total IS 'Aggregated stats for snapshot, based on pg_stat_statements';
CREATE TABLE tables_list(
    relid       oid,
    schemaname  name,
    relname     name,
    CONSTRAINT pk_tables_list PRIMARY KEY (relid)
);
COMMENT ON TABLE tables_list IS 'Table names and scheams, captured in snapshots';

CREATE VIEW v_snap_stat_user_tables AS
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
    FROM ONLY snap_stat_user_tables JOIN tables_list USING (relid);
COMMENT ON VIEW v_snap_stat_user_tables IS 'Reconstructed stats view with table names and schemas';
CREATE TABLE last_stat_user_tables AS SELECT * FROM v_snap_stat_user_tables WHERE 0=1;
COMMENT ON TABLE last_stat_user_tables IS 'Last snapshot data for calculating diffs in next snapshot';
CREATE TABLE indexes_list(
    indexrelid      oid,
    schemaname      name,
    indexrelname    name,
    CONSTRAINT pk_indexes_list PRIMARY KEY (indexrelid)
);
COMMENT ON TABLE indexes_list IS 'Index names and scheams, captured in snapshots';
CREATE TABLE snap_stat_user_indexes (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    relid oid REFERENCES tables_list(relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    indexrelid oid REFERENCES indexes_list(indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    relsize bigint,
    relsize_diff bigint,
    indisunique bool,
    CONSTRAINT pk_snap_stat_user_indexes PRIMARY KEY (snap_id,dbid,relid,indexrelid)
);
COMMENT ON TABLE snap_stat_user_indexes IS 'Stats increments for user indexes in all databases by snapshots';

CREATE VIEW v_snap_stat_user_indexes AS
    SELECT
        snap_id,
        dbid,
        s.relid,
        s.indexrelid,
        il.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        relsize,
        relsize_diff,
        indisunique
    FROM ONLY 
        snap_stat_user_indexes s 
        JOIN indexes_list il ON (il.indexrelid = s.indexrelid)
        JOIN tables_list tl ON (tl.relid = s.relid);
COMMENT ON VIEW v_snap_stat_user_indexes IS 'Reconstructed stats view with table and index names and schemas';
CREATE TABLE last_stat_user_indexes AS SELECT * FROM v_snap_stat_user_indexes WHERE 0=1;
COMMENT ON TABLE last_stat_user_indexes IS 'Last snapshot data for calculating diffs in next snapshot';
CREATE TABLE funcs_list(
    funcid      oid,
    schemaname  name,
    funcname    name,
    CONSTRAINT pk_funcs_list PRIMARY KEY (funcid)
);
COMMENT ON TABLE funcs_list IS 'Function names and scheams, captured in snapshots';
CREATE TABLE snap_stat_user_functions (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    funcid oid REFERENCES funcs_list(funcid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    calls bigint,
    total_time double precision,
    self_time double precision,
    CONSTRAINT pk_snap_stat_user_functions PRIMARY KEY (snap_id,dbid,funcid)
);
COMMENT ON TABLE snap_stat_user_functions IS 'Stats increments for user functions in all databases by snapshots';
CREATE VIEW v_snap_stat_user_functions AS
    SELECT
        snap_id,
        dbid,
        funcid,
        schemaname,
        funcname,
        calls,
        total_time,
        self_time
    FROM ONLY snap_stat_user_functions JOIN funcs_list USING (funcid);
COMMENT ON VIEW v_snap_stat_user_indexes IS 'Reconstructed stats view with function names and schemas';
CREATE TABLE snap_statio_user_tables (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    relid oid REFERENCES tables_list(relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT pk_snap_statio_user_tables PRIMARY KEY (snap_id,dbid,relid)
);
COMMENT ON TABLE snap_statio_user_tables IS 'IO Stats increments for user tables in all databases by snapshots';
CREATE VIEW v_snap_statio_user_tables AS
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
    FROM ONLY snap_statio_user_tables JOIN tables_list USING (relid);
COMMENT ON VIEW v_snap_statio_user_tables IS 'Reconstructed stats view with table names and schemas';

CREATE TABLE last_stat_user_functions AS SELECT * FROM v_snap_stat_user_functions WHERE 0=1;
COMMENT ON TABLE last_stat_user_functions IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_statio_user_indexes (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    dbid oid,
    relid oid REFERENCES tables_list(relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    indexrelid oid REFERENCES indexes_list(indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT pk_snap_statio_user_indexes PRIMARY KEY (snap_id,dbid,relid,indexrelid)
);
COMMENT ON TABLE snap_statio_user_indexes IS 'Stats increments for user indexes in all databases by snapshots';
CREATE VIEW v_snap_statio_user_indexes AS
    SELECT
        snap_id,
        dbid,
        s.relid,
        s.indexrelid,
        il.schemaname,
        tl.relname,
        il.indexrelname,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff
    FROM
        ONLY snap_statio_user_indexes s
        JOIN tables_list tl ON (s.relid = tl.relid)
        JOIN indexes_list il ON (s.indexrelid = il.indexrelid);
COMMENT ON VIEW v_snap_statio_user_indexes IS 'Reconstructed stats view with table and index names and schemas';
CREATE TABLE last_statio_user_indexes AS SELECT * FROM v_snap_statio_user_indexes WHERE 0=1;
COMMENT ON TABLE last_statio_user_indexes IS 'Last snapshot data for calculating diffs in next snapshot';
CREATE TABLE snap_stat_database
(
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    datid oid,
    datname name,
    xact_commit bigint,
    xact_rollback bigint,
    blks_read bigint,
    blks_hit bigint,
    tup_returned bigint,
    tup_fetched bigint,
    tup_inserted bigint,
    tup_updated bigint,
    tup_deleted bigint,
    conflicts bigint,
    temp_files bigint,
    temp_bytes bigint,
    deadlocks bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    stats_reset timestamp with time zone,
    datsize_delta bigint,
    CONSTRAINT pk_snap_stat_database PRIMARY KEY (snap_id,datid,datname)
);
COMMENT ON TABLE snap_stat_database IS 'Snapshot database statistics table (fields from pg_stat_database)';CREATE TABLE last_stat_database AS SELECT * FROM snap_stat_database WHERE 0=1;
COMMENT ON TABLE last_stat_database IS 'Last snapshot data for calculating diffs in next snapshot';
CREATE TABLE snap_stat_cluster
(
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE CASCADE,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamp with time zone,
    wal_size bigint,
    CONSTRAINT pk_snap_stat_cluster PRIMARY KEY (snap_id)
);
COMMENT ON TABLE snap_stat_cluster IS 'Snapshot cluster statistics table (fields from pg_stat_bgwriter, etc.)';
CREATE TABLE last_stat_cluster AS SELECT * FROM snap_stat_cluster WHERE 0=1;
COMMENT ON TABLE last_stat_cluster IS 'Last snapshot data for calculating diffs in next snapshot';
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
CREATE OR REPLACE FUNCTION collect_obj_stats(IN s_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    --Cursor for db stats
    c_dblist CURSOR FOR
    select dbs.datid,dbs.datname,s1.setting as port from pg_catalog.pg_stat_database dbs, pg_catalog.pg_settings s1
    where dbs.datname not like 'template_' and s1.name='port';

	r_result RECORD;
BEGIN
    -- Creating temporary tables, holding data for objects of all cluster databases
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_stat_user_tables (
        snap_id integer,
        dbid oid,
        relid oid,
        schemaname name,
        relname name,
        seq_scan bigint,
        seq_tup_read bigint,
        idx_scan bigint,
        idx_tup_fetch bigint,
        n_tup_ins bigint,
        n_tup_upd bigint,
        n_tup_del bigint,
        n_tup_hot_upd bigint,
        n_live_tup bigint,
        n_dead_tup bigint,
        n_mod_since_analyze bigint,
        last_vacuum timestamp with time zone,
        last_autovacuum timestamp with time zone,
        last_analyze timestamp with time zone,
        last_autoanalyze timestamp with time zone,
        vacuum_count bigint,
        autovacuum_count bigint,
        analyze_count bigint,
        autoanalyze_count bigint,
        relsize bigint,
        relsize_diff bigint
    ) ON COMMIT DROP;
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_stat_user_functions (
        snap_id integer,
        dbid oid,
        funcid oid,
        schemaname name,
        funcname name,
        calls bigint,
        total_time double precision,
        self_time double precision
    ) ON COMMIT DROP;
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_stat_user_indexes (
        snap_id integer,
        dbid oid,
        relid oid,
        indexrelid oid,
        schemaname name,
        relname name,
        indexrelname name,
        idx_scan bigint,
        idx_tup_read bigint,
        idx_tup_fetch bigint,
        relsize bigint,
        relsize_diff bigint,
        indisunique bool
    ) ON COMMIT DROP;
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_statio_user_tables (
        snap_id integer,
        dbid oid,
        relid oid,
        schemaname name,
        relname name,
        heap_blks_read bigint,
        heap_blks_hit bigint,
        idx_blks_read bigint,
        idx_blks_hit bigint,
        toast_blks_read bigint,
        toast_blks_hit bigint,
        tidx_blks_read bigint,
        tidx_blks_hit bigint,
        relsize bigint,
        relsize_diff bigint
    ) ON COMMIT DROP;
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_statio_user_indexes (
        snap_id integer,
        dbid oid,
        relid oid,
        indexrelid oid,
        schemaname name,
        relname name,
        indexrelname name,
        idx_blks_read bigint,
        idx_blks_hit bigint,
        relsize bigint,
        relsize_diff bigint
    ) ON COMMIT DROP;

   -- Load new data from statistic views of all cluster databases
	FOR r_result IN c_dblist LOOP
      INSERT INTO temp_stat_user_tables
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select *,pg_relation_size(relid) relsize,0 relsize_diff from pg_catalog.pg_stat_user_tables')
      AS t (
         relid oid,
         schemaname name,
         relname name,
         seq_scan bigint,
         seq_tup_read bigint,
         idx_scan bigint,
         idx_tup_fetch bigint,
         n_tup_ins bigint,
         n_tup_upd bigint,
         n_tup_del bigint,
         n_tup_hot_upd bigint,
         n_live_tup bigint,
         n_dead_tup bigint,
         n_mod_since_analyze bigint,
         last_vacuum timestamp with time zone,
         last_autovacuum timestamp with time zone,
         last_analyze timestamp with time zone,
         last_autoanalyze timestamp with time zone,
         vacuum_count bigint,
         autovacuum_count bigint,
         analyze_count bigint,
         autoanalyze_count bigint,
         relsize bigint,
         relsize_diff bigint
      );
      
      INSERT INTO temp_stat_user_indexes
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select st.*,pg_relation_size(st.relid),0,(ix.indisunique or con.conindid IS NOT NULL) as indisunique
from pg_catalog.pg_stat_user_indexes st 
join pg_catalog.pg_index ix on (ix.indexrelid = st.indexrelid) 
left join pg_catalog.pg_constraint con on(con.conindid = ix.indexrelid and con.contype in (''p'',''u''))')
      AS t (
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         idx_scan bigint,
         idx_tup_read bigint,
         idx_tup_fetch bigint,
         relsize bigint,
         relsize_diff bigint,
         indisunique bool
      );
      
      INSERT INTO temp_stat_user_functions
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select * from pg_catalog.pg_stat_user_functions')
      AS t (
         funcid oid,
         schemaname name,
         funcname name,
         calls bigint,
         total_time double precision,
         self_time double precision
      );

      INSERT INTO temp_statio_user_tables
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select *,pg_relation_size(relid),0 from pg_catalog.pg_statio_user_tables')
      AS t (
         relid oid,
         schemaname name,
         relname name,
         heap_blks_read bigint,
         heap_blks_hit bigint,
         idx_blks_read bigint,
         idx_blks_hit bigint,
         toast_blks_read bigint,
         toast_blks_hit bigint,
         tidx_blks_read bigint,
         tidx_blks_hit bigint,
         relsize bigint,
         relsize_diff bigint
      );
      
      INSERT INTO temp_statio_user_indexes
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select *,pg_relation_size(relid),0 from pg_catalog.pg_statio_user_indexes')
      AS t (
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         idx_blks_read bigint,
         idx_blks_hit bigint,
         relsize bigint,
         relsize_diff bigint
      );
	END LOOP;
   RETURN 0;
END;
$$ LANGUAGE plpgsql; 
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
CREATE OR REPLACE FUNCTION snapshot_show(IN days integer = NULL) RETURNS TABLE(snapshot integer, date_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT snap_id, snap_time
    FROM snapshots
    WHERE days IS NULL OR snap_time > now() - (days || ' days')::interval
    ORDER BY snap_id;
$$ LANGUAGE SQL;
/* ========= Baseline management functions ========= */

CREATE OR REPLACE FUNCTION baseline_new(IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    baseline_id integer;
BEGIN
    INSERT INTO baselines(bl_name,keep_until)
    VALUES (name,now() + (days || ' days')::interval)
    RETURNING bl_id INTO baseline_id;

    INSERT INTO bl_snaps (snap_id,bl_id)
    SELECT snap_id, baseline_id
    FROM snapshots
    WHERE snap_id BETWEEN start_id AND end_id;

    RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_drop(IN name varchar(25) = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM baselines WHERE name IS NULL OR bl_name = name;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_keep(IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE name IS NULL OR bl_name = name;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_show() RETURNS TABLE(baseline varchar(25), min_snap integer, max_snap integer, keep_until_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT bl_name as baseline,min_snap_id,max_snap_id, keep_until 
    FROM baselines b JOIN 
        (SELECT bl_id,min(snap_id) min_snap_id,max(snap_id) max_snap_id FROM bl_snaps GROUP BY bl_id) b_agg
    USING (bl_id)
    ORDER BY min_snap_id;
$$ LANGUAGE SQL;
/* ========= Reporting functions ========= */

/* ===== Cluster report functions ===== */
CREATE OR REPLACE FUNCTION dbstats_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Commits</th><th>Rollbacks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Growth</th><th>Deadlocks</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT 
        datname as dbname,
        sum(xact_commit) as xact_commit,
        sum(xact_rollback) as xact_rollback,
        sum(blks_read) as blks_read,
        sum(blks_hit) as blks_hit,
        sum(tup_returned) as tup_returned,
        sum(tup_fetched) as tup_fetched,
        sum(tup_inserted) as tup_inserted,
        sum(tup_updated) as tup_updated,
        sum(tup_deleted) as tup_deleted,
        sum(temp_files) as temp_files,
        pg_size_pretty(sum(temp_bytes)) as temp_bytes,
        pg_size_pretty(sum(datsize_delta)) as datsize_delta,
        sum(deadlocks) as deadlocks, 
        sum(blks_hit)*100/GREATEST(sum(blks_hit)+sum(blks_read),1) as blks_hit_pct
    FROM snap_stat_database
    WHERE datname not like 'template_' and snap_id between s_id + 1 and e_id
    GROUP BY datid,datname
    HAVING max(stats_reset)=min(stats_reset);

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize_delta,
            r_result.deadlocks
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION statements_stats_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT 
        db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.shared_blks_hit + st.shared_blks_read) as shared_gets,
        sum(st.local_blks_hit + st.local_blks_read) as local_gets,
        sum(st.shared_blks_dirtied) as shared_blks_dirtied,
        sum(st.local_blks_dirtied) as local_blks_dirtied,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.statements) as statements
    FROM snap_statements_total st 
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id)
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY ROLLUP(db_s.datname)
    ORDER BY db_s.datname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION cluster_stats_htbl(IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Metric</th><th>Value</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT
        sum(checkpoints_timed) as checkpoints_timed,
        sum(checkpoints_req) as checkpoints_req,
        sum(checkpoint_write_time) as checkpoint_write_time,
        sum(checkpoint_sync_time) as checkpoint_sync_time,
        sum(buffers_checkpoint) as buffers_checkpoint,
        sum(buffers_clean) as buffers_clean,
        sum(buffers_backend) as buffers_backend,
        sum(buffers_backend_fsync) as buffers_backend_fsync,
        sum(maxwritten_clean) as maxwritten_clean,
        sum(buffers_alloc) as buffers_alloc,
        pg_size_pretty(sum(wal_size)) as wal_size
    FROM snap_stat_cluster
    WHERE snap_id between s_id + 1 and e_id
    HAVING max(stats_reset)=min(stats_reset);

    r_result RECORD;
BEGIN
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(row_tpl,'Scheduled checkpoints',r_result.checkpoints_timed);
        report := report||format(row_tpl,'Requested checkpoints',r_result.checkpoints_req);
        report := report||format(row_tpl,'Checkpoint write time (s)',round(cast(r_result.checkpoint_write_time/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoint sync time (s)',round(cast(r_result.checkpoint_sync_time/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoints pages written',r_result.buffers_checkpoint);
        report := report||format(row_tpl,'Background pages written',r_result.buffers_clean);
        report := report||format(row_tpl,'Backend pages written',r_result.buffers_backend);
        report := report||format(row_tpl,'Backend fsync count',r_result.buffers_backend_fsync);
        report := report||format(row_tpl,'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean);
        report := report||format(row_tpl,'Number of buffers allocated',r_result.buffers_alloc);
        report := report||format(row_tpl,'WAL generated',r_result.wal_size);
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

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
CREATE OR REPLACE FUNCTION top_dml_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th></tr>{rows}</table>';
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
    HAVING sum(n_tup_ins)+sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) > 0
    ORDER BY sum(n_tup_ins)+sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) DESC
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
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd,
            r_result.seq_scan,
            r_result.seq_tup_read,
            r_result.idx_scan,
            r_result.idx_tup_fetch
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_upd_vac_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Upd</th><th>Upd(HOT)</th><th>Del</th><th>Vacuum</th><th>AutoVacuum</th><th>Analyze</th><th>AutoAnalyze</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        schemaname,
        relname,
        sum(n_tup_upd)-sum(n_tup_hot_upd) as n_tup_upd,
        sum(n_tup_del) as n_tup_del,
        sum(n_tup_hot_upd) as n_tup_hot_upd,
        sum(vacuum_count) as vacuum_count,
        sum(autovacuum_count) as autovacuum_count,
        sum(analyze_count) as analyze_count,
        sum(autoanalyze_count) as autoanalyze_count
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
    HAVING sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) > 0
    ORDER BY sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) DESC
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
            r_result.n_tup_upd,
            r_result.n_tup_hot_upd,
            r_result.n_tup_del,
            r_result.vacuum_count,
            r_result.autovacuum_count,
            r_result.analyze_count,
            r_result.autoanalyze_count
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
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
CREATE OR REPLACE FUNCTION top_growth_indexes_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>Size</th><th>Growth</th><th>Scans</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.relname,
        st.indexrelname,
        sum(st.idx_scan) as idx_scan,
        pg_size_pretty(sum(st.relsize_diff)) as growth,
        pg_size_pretty(max(st_last.relsize)) as relsize
    FROM v_snap_stat_user_indexes st
        JOIN v_snap_stat_user_indexes st_last using (dbid,relid,indexrelid)
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
        AND st_last.snap_id=db_e.snap_id
    GROUP BY db_s.datid,relid,indexrelid,db_s.datname,st.schemaname,st.relname,st.indexrelname
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
            r_result.indexrelname,
            r_result.relsize,
            r_result.growth,
            r_result.idx_scan
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
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
CREATE OR REPLACE FUNCTION tbl_top_io_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Heap</th><th>Ix</th><th>TOAST</th><th>TOAST-Ix</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read) as heap_blks_read,
        sum(st.idx_blks_read) as idx_blks_read,
        sum(st.toast_blks_read) as toast_blks_read,
        sum(st.tidx_blks_read) as tidx_blks_read
    FROM v_snap_statio_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,st.schemaname,st.relname
    ORDER BY sum(st.heap_blks_read + st.idx_blks_read + st.toast_blks_read + st.tidx_blks_read) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read,
            r_result.idx_blks_read,
            r_result.toast_blks_read,
            r_result.tidx_blks_read
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
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
/* ===== Functions report ===== */

CREATE OR REPLACE FUNCTION func_top_time_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.funcname,
        sum(st.calls) as calls,
        sum(st.total_time) as total_time,
        sum(st.self_time) as self_time,
        sum(st.total_time)/sum(st.calls) as m_time,
        sum(st.self_time)/sum(st.calls) as m_stime
    FROM v_snap_stat_user_functions st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
    ORDER BY sum(st.total_time) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION func_top_calls_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.funcname,
        sum(st.calls) as calls,
        sum(st.total_time) as total_time,
        sum(st.self_time) as self_time,
        sum(st.total_time)/sum(st.calls) as m_time,
        sum(st.self_time)/sum(st.calls) as m_stime
    FROM v_snap_stat_user_functions st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
    ORDER BY sum(st.calls) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

/* ===== Statements report ===== */
CREATE OR REPLACE FUNCTION check_stmt_cnt(IN start_id integer = 0, IN end_id integer = 0) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tab_tpl CONSTANT text := '<table><tr><th>Snapshot ID</th><th>Snapshot Time</th><th>Stmts Captured</th><th>pg_stat_statements.max</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    report text := '';

    c_stmt_all_stats CURSOR FOR
    SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt 
    FROM snap_params prm 
        JOIN (
            SELECT snap_id,sum(statements) stmt_cnt
            FROM snap_statements_total
            GROUP BY snap_id
        ) snap_stmt_cnt USING(snap_id)
        JOIN snapshots USING (snap_id)
    WHERE prm.p_name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

    c_stmt_stats CURSOR (s_id integer, e_id integer) FOR
    SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt
    FROM snap_params prm 
        JOIN (
            SELECT snap_id,sum(statements) stmt_cnt
            FROM snap_statements_total
            WHERE snap_id BETWEEN s_id AND e_id
            GROUP BY snap_id
        ) snap_stmt_cnt USING(snap_id)
        JOIN snapshots USING (snap_id)
    WHERE prm.p_name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

    r_result RECORD;
BEGIN
    IF start_id = 0 THEN
        FOR r_result IN c_stmt_all_stats LOOP
            report := report||format(
                row_tpl,
                r_result.snap_id,
                r_result.snap_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    ELSE
        FOR r_result IN c_stmt_stats(start_id,end_id) LOOP
            report := report||format(
                row_tpl,
                r_result.snap_id,
                r_result.snap_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    END IF; 

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Elapsed time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) queries ordered by epapsed time 
    c_elapsed_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(total_time),1) AS total_time
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id)
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.total_time*100/tot.total_time) as total_pct,
        min(st.min_time) as min_time,max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) as shared_blks_hit,
        sum(st.shared_blks_read) as shared_blks_read,
        sum(st.shared_blks_dirtied) as shared_blks_dirtied,
        sum(st.shared_blks_written) as shared_blks_written,
        sum(st.local_blks_hit) as local_blks_hit,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_dirtied) as local_blks_dirtied,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.blk_read_time) as blk_read_time,
        sum(st.blk_write_time) as blk_write_time
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    ORDER BY total_time DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.total_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION top_exec_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Executions sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Executions</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Total(s)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by executions 
    c_calls CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(calls),1) AS calls
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.calls/tot.calls)*100 as total_pct,
        sum(st.total_time)/1000 as total_time,
        min(st.min_time) as min_time,
        max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows) as rows
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    ORDER BY calls DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.total_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            round(CAST(r_result.total_time AS numeric),1)
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION top_iowait_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- IOWait time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>IO wait(s)</th><th>%Total</th><th>Reads</th><th>Writes</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by I/O Wait time 
    c_iowait_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT 
            CASE WHEN sum(blk_read_time) = 0 THEN 1 ELSE sum(blk_read_time) END AS blk_read_time,
            CASE WHEN sum(blk_write_time) = 0 THEN 1 ELSE sum(blk_write_time) END AS blk_write_time
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) as shared_blks_hit,
        sum(st.shared_blks_read) as shared_blks_read,
        sum(st.shared_blks_dirtied) as shared_blks_dirtied,
        sum(st.shared_blks_written) as shared_blks_written,
        sum(st.local_blks_hit) as local_blks_hit,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_dirtied) as local_blks_dirtied,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.blk_read_time) as blk_read_time,
        sum(st.blk_write_time) as blk_write_time,
        (sum(st.blk_read_time + st.blk_write_time))/1000 as io_time,
        (sum(st.blk_read_time + st.blk_write_time)*100/min(tot.blk_read_time+tot.blk_write_time)) as total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.blk_read_time) + sum(st.blk_write_time) > 0
    ORDER BY io_time DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.total_pct AS numeric),2),
            round(CAST(r_result.shared_blks_read AS numeric)),
            round(CAST(r_result.shared_blks_written AS numeric)),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION top_gets_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Gets sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by gets
    c_gets CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(shared_blks_hit),1) AS shared_blks_hit,
            GREATEST(sum(shared_blks_read),1) AS shared_blks_read
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
        (sum(st.shared_blks_hit + st.shared_blks_read)*100/min(tot.shared_blks_read + tot.shared_blks_hit)) as total_pct,
        sum(st.shared_blks_hit) * 100 / CASE WHEN (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) = 0 THEN 1
            ELSE (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) END as hit_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.shared_blks_hit) + sum(st.shared_blks_read) > 0
    ORDER BY gets DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by gets
    FOR r_result IN c_gets(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.total_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_temp_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Temp usage sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Work_w(blk)</th><th>%Total</th><th>Work_r(blk)</th><th>%Total</th><th>Local_w(blk)</th><th>%Total</th><th>Local_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by temp usage 
    c_temp CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT
            GREATEST(sum(temp_blks_read),1) AS temp_blks_read,
            GREATEST(sum(temp_blks_written),1) AS temp_blks_written,
            GREATEST(sum(local_blks_read),1) AS local_blks_read,
            GREATEST(sum(local_blks_written),1) AS local_blks_written
        FROM snap_statements_total
        WHERE snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
        sum(st.shared_blks_hit) * 100 / GREATEST(sum(st.shared_blks_hit)+sum(st.shared_blks_read),1) as hit_pct,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read*100/tot.temp_blks_read) as temp_read_total_pct,
        sum(st.temp_blks_written*100/tot.temp_blks_written) as temp_write_total_pct,
        sum(st.local_blks_read*100/tot.local_blks_read) as local_read_total_pct,
        sum(st.local_blks_written*100/tot.local_blks_written) as local_write_total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written) > 0
    ORDER BY sum(st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collect_queries(IN query_id char(10), IN query_text text) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    INSERT INTO queries_list
    VALUES (query_id,regexp_replace(query_text,'\s+',' ','g'))
    ON CONFLICT DO NOTHING;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;
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

CREATE OR REPLACE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    IF section_text IS NULL OR section_text = '' THEN
        RETURN '<p>No data in this section</p>';
    ELSE
        RETURN section_text;
    END IF;
END;
$$ LANGUAGE plpgsql;
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
