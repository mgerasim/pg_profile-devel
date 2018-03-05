CREATE OR REPLACE FUNCTION test_cluster_remove() RETURNS bigint
SET search_path=public
AS $$
DECLARE
    count_before    integer;
    count_after     integer;
    ID              bigint;
BEGIN
    
    SELECT INTO count_before count(*) FROM clusters;
    SELECT INTO ID cluster_add('test1', 'username', 'password', 'host1', 5432, 'postgres');
    SELECT INTO count_after count(*) FROM clusters;
    
    IF ((count_after - count_before) = 1) THEN
        raise notice 'Test success';
    ELSE
        raise notice 'Test fail';
    END IF;
    
    PERFORM cluster_remove('test1');
    
    SELECT INTO count_after count(*) FROM clusters;
    
    IF ((count_after - count_before) = 0) THEN
        raise notice 'Test success';
    ELSE
        raise notice 'Test fail';
    END IF;
        
    PERFORM cluster_remove('test1');
    EXCEPTION
        WHEN others THEN
            IF (SQLSTATE = 'P0001') THEN
                raise notice 'Test success';
            ELSE
                raise notice 'Test fail';
            END IF;
    
    RETURN 1;            
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION test_cluster_remove() IS 'Testing of function cluster_remove of removing of a cluster of databases';
BEGIN;
SELECT test_cluster_remove();
ROLLBACK;