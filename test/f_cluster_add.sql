CREATE OR REPLACE FUNCTION test_cluster_add() RETURNS bigint
SET search_path=public
AS $$
DECLARE
    count_before    integer;
    count_after     integer;
    ID              bigint;
BEGIN
    SELECT INTO count_before count(*) FROM clusters;
    SELECT INTO ID cluster_add('test1', 'username', 'password', 'host1', 5432, 'postgres');
    SELECT INTO ID cluster_add('test2', 'username', 'password', 'host2', 5432);
    SELECT INTO ID cluster_add('test3', 'username', 'password', 'host3');
    SELECT INTO count_after count(*) FROM clusters;
    
    IF ((count_after - count_before) = 3) THEN
        raise notice 'Test success';
    ELSE
        raise notice 'Test fail';
    END IF;
            
    RETURN 1;            
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION test_cluster_add() IS 'Testing of function cluster_add of adding of a cluster of databases';
BEGIN;
SELECT test_cluster_add();
ROLLBACK;