-- Function CLUSTER_DISABLE - disable create snapshot of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:
-- ID                       -   identifier of cluster of databases
CREATE OR REPLACE FUNCTION cluster_disable(IN _ID bigint) RETURNS void
SET search_path=public
AS $$
DECLARE
    count_updated integer;
BEGIN
    UPDATE clusters 
        SET is_enabled = false
    WHERE cluster_id = _ID;
    GET DIAGNOSTICS count_updated = ROW_COUNT;
    IF (count_updated = 1) THEN
        RAISE NOTICE 'Cluster of databases with ID=% is disabled', _ID;
    ELSE
        RAISE 'Cluster of databases % not found for disable', _ID
            USING HINT = 'Check the input parameter _ID for function cluster_disable';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_disable(IN _ID bigint) IS 'Disable create snapshot of a cluster of databases';