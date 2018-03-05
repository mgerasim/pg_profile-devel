CREATE OR REPLACE FUNCTION test_cluster_edit() RETURNS bigint
SET search_path=public
AS $$
DECLARE
    count_before    integer;
    count_after     integer;
    ID              bigint;
    temp_ID         bigint;
    cluster_username    varchar;
BEGIN
    
    SELECT INTO count_before count(*) FROM clusters;
    SELECT INTO ID cluster_add('test1', 'username', 'password', 'host1', 5432, 'postgres');
    
    PERFORM cluster_edit('test1', 'username1', 'password', 'host1', 5432, 'postgres');
    
    SELECT connect_username INTO cluster_username  FROM clusters WHERE cluster_id = ID;
        
    IF (cluster_username = 'username1') THEN 
        raise notice 'Test success';
    ELSE
        raise notice 'Test fail';
    END IF;
    
    
    PERFORM cluster_edit('test2', 'username1', 'password', 'host1', 5432, 'postgres');
    
    RETURN 1;
    
    EXCEPTION
        WHEN others THEN
            IF (SQLSTATE = 'P0001') THEN
                raise notice 'Test success exception';
            ELSE
                raise notice 'Test fail exception %:%', SQLSTATE, SQLERRM;
            END IF;
    
    RETURN 1;            
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION test_cluster_edit() IS 'Testing of function cluster_edit of updating of a cluster of databases';
BEGIN;
SELECT test_cluster_edit();
ROLLBACK;