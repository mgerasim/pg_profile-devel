CREATE TABLE stmt_list(
    queryid_md5    char(10),
    query          text,
    CONSTRAINT pk_snap_users PRIMARY KEY (queryid_md5)
);
COMMENT ON TABLE stmt_list IS 'Statements, captured in snapshots';
