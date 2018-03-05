-- Function CLUSTER_DISABLE - disable create snapshot of a cluster of databases, if not found then exception with code P0001
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

COMMENT ON FUNCTION cluster_disable() IS 'Disable create snapshot of a all clusters of databases';