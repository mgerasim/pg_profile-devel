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
