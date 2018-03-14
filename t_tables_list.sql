CREATE TABLE tables_list(
    relid       oid,
    schemaname  name,
    relname     name,
    CONSTRAINT pk_tables_list PRIMARY KEY (relid)
);
COMMENT ON TABLE tables_list IS 'Table names and scheams, captured in snapshots';
