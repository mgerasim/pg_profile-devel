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