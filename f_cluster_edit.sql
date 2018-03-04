-- Function CLUSTER_EDIT - updating fields of a cluster of databases, if not found then exception with code P0001
-- INPUT PARAMETERS:
-- ID                       -   identifier of cluster of databases                       
-- _new_name                -   name of cluster of databases for updating
-- _new_connect_username    -   username of cluster of databases for updating
-- _new_connect_password    -   password of cluster of databases for updating
-- _new_connect_host        -   host of cluster of databases for updating
-- _new_connect_port        -   port of cluster of databases for updating
-- _new_connect_database    -   database of cluster of databases for updating
CREATE OR REPLACE FUNCTION cluster_edit(IN _ID bigint,
                                        IN _new_name varchar,
                                        IN _new_connect_username varchar,
                                        IN _new_connect_password varchar, 
                                        IN _new_connect_host varchar, 
                                        IN _new_connect_port integer DEFAULT 5432,
                                        IN _new_connect_database varchar DEFAULT 'postgres') RETURNS void
SET search_path=public
AS $$
DECLARE
    count_updtated integer;
BEGIN
    UPDATE clusters 
        SET name = _new_name,
            connect_username = _new_connect_username,
            connect_password = _new_connect_password,
            connect_host = _new_connect_host,
            connect_port = _new_connect_port,
            connect_database = _new_connect_database
    WHERE cluster_id = _ID;
    GET DIAGNOSTICS count_updtated = ROW_COUNT;
    IF (count_updtated = 1) THEN
        RAISE NOTICE 'Cluster of databases with ID=% is updated', _ID;
    ELSE
        RAISE 'Cluster of databases % not found for updating', _ID
            USING HINT = 'Check the input parameter _ID for function cluster_edit';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_edit(IN _ID bigint,
                                 IN _new_name varchar,
                                 IN _new_connect_username varchar,
                                 IN _new_connect_password varchar, 
                                 IN _new_connect_host varchar, 
                                 IN _new_connect_port integer,
                                 IN _new_connect_database varchar) IS 'Updating fields of a cluster of databases';