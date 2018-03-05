-- Function CLUSTER_REMOVE - removing of a cluster of databases
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

