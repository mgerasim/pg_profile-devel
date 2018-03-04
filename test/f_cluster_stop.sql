CREATE OR REPLACE FUNCTION test_cluster_stop() RETURNS bigint
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
    
    PERFORM cluster_stop(ID);
    
    raise notice 'Test success';
    RETURN 1;            
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION test_cluster_start() IS 'Testing of function cluster_stop of stoping of a cluster of databases';
BEGIN;
SELECT test_cluster_stop();
ROLLBACK;