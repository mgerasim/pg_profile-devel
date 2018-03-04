-- Function CLUSTER_EDIT_NAME - updating name field of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:
-- ID           -   identifier of cluster of databases                       
-- _new_name    -   name cluster of databases for removing
CREATE OR REPLACE FUNCTION cluster_edit_name(IN _ID bigint, IN _new_name varchar) RETURNS void 
SET search_path=public
AS $$
DECLARE
    count_updtated integer;
BEGIN
    UPDATE clusters SET name = _new_name WHERE cluster_id = _ID;
    GET DIAGNOSTICS count_updtated = ROW_COUNT;
    IF (count_updtated = 1) THEN
        RAISE NOTICE 'Cluster of databases with ID=% is updated', _ID;
    ELSE
        RAISE 'Cluster of databases % not found for updating', _ID
            USING HINT = 'Check the input parameter _ID for function cluster_edit_name(_ID, _new_name)';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_edit_name(IN _ID bigint, IN _new_name varchar) IS 'Updating name field of a cluster of databases';

