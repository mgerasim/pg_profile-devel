CREATE TABLE funcs_list(
    funcid      oid,
    schemaname  name,
    funcname    name,
    CONSTRAINT pk_funcs_list PRIMARY KEY (funcid)
);
COMMENT ON TABLE funcs_list IS 'Function names and scheams, captured in snapshots';
