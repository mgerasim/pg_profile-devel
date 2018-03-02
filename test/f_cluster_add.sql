CREATE OR REPLACE FUNCTION test_cluster_add() RETURNS bigint
AS $$
DECLARE
    count_before    integer;
	count_after		integer;
	ID 				bigint;
BEGIN

	SELECT INTO count_before count(*) FROM clusters;

	SELECT INTO ID cluster_add('test', 5361, 'dbname', 'username', 'password', 'description');
	SELECT INTO ID cluster_add('test1', 'username', 'password', 'description');
	SELECT INTO ID cluster_add('test2', 'dbname', 'username', 'password', 'description');
	SELECT INTO ID cluster_add();

	SELECT INTO count_after count(*) FROM clusters;
	
	IF ((count_after - count_before) = 4) THEN
		raise notice 'Test success';
	ELSE
		raise notice 'Test fail';
	END IF;
			
	RETURN 1;

			
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION test_cluster_add() IS '������������ ������� ���������� ��������.';
BEGIN;
SELECT test_cluster_add();
ROLLBACK;