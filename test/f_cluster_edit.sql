CREATE OR REPLACE FUNCTION test_cluster_edit() RETURNS bigint
SET search_path=public
AS $$
DECLARE
    count_before    integer;
    count_after     integer;
    ID              bigint;
    temp_ID         bigint;
    cluster_name    varchar;
BEGIN
    
    SELECT INTO count_before count(*) FROM clusters;
    SELECT INTO ID cluster_add('test1', 'username', 'password', 'host1', 5432, 'postgres');
    
    PERFORM cluster_edit_name(ID, 'test_update');
    
    SELECT name INTO cluster_name  FROM clusters WHERE cluster_id = ID;
        
    IF (cluster_name = 'test_update') THEN 
        raise notice 'Test success';
    ELSE
        raise notice 'Test fail';
    END IF;
    
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

COMMENT ON FUNCTION test_cluster_edit() IS 'Testing of function cluster_edit_* of updating of a cluster of databases';
BEGIN;
SELECT test_cluster_edit();
ROLLBACK;