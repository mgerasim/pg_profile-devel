CREATE TABLE indexes_list(
    indexrelid      oid,
    schemaname      name,
    indexrelname    name,
    CONSTRAINT pk_indexes_list PRIMARY KEY (indexrelid)
);
COMMENT ON TABLE indexes_list IS 'Index names and scheams, captured in snapshots';
