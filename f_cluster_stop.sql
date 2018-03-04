-- Function CLUSTER_STOP - stoping create snapshot of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:
-- ID                       -   identifier of cluster of databases
CREATE OR REPLACE FUNCTION cluster_stop(IN _ID bigint) RETURNS void
SET search_path=public
AS $$
DECLARE
    count_updtated integer;
BEGIN
    UPDATE clusters 
        SET is_started = false
    WHERE cluster_id = _ID;
    GET DIAGNOSTICS count_updtated = ROW_COUNT;
    IF (count_updtated = 1) THEN
        RAISE NOTICE 'Cluster of databases with ID=% is stoped', _ID;
    ELSE
        RAISE 'Cluster of databases % not found for stoping', _ID
            USING HINT = 'Check the input parameter _ID for function cluster_stop';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_stop(IN _ID bigint) IS 'Stoping create snapshot of a cluster of databases';