-- ���������� �������� ��� ������
-- connect_host - ���� �������� ���� ������
-- connect_port - ���� ��� ����������� � �������� ���� ������
-- connect_database - ��� ���� ������ ��� ���������� ����������� � ��������
-- username - ������������ ��� ����������� � �������� ���� ������
-- password - ������ ��� ������������ 
-- description - �������� �������� ���� ������
CREATE OR REPLACE FUNCTION cluster_add(IN connect_host varchar, 
									IN connect_port integer, 
									IN connect_database varchar, 
									IN username varchar, 
									IN password varchar, 
									IN description text) RETURNS bigint
AS $$
DECLARE
    ID    bigint;
BEGIN
	INSERT INTO clusters( 	connect_host, 
						connect_port, 
						connect_database, 
						username, 
						password, 
						description) 
			VALUES (	connect_host, 
						connect_port,
						connect_database,
						username,
						password,
						description)
			RETURNING cluster_id 
			INTO ID;
			
	RETURN ID;
				
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add(IN connect_host varchar, 
									IN connect_port integer, 
									IN connect_database varchar, 
									IN username varchar, 
									IN password varchar, 
									IN description text) IS '���������� ��������.';

-- ���������� �������� ��� ������
-- connect_host - ���� �������� ���� ������
-- connect_port - �� ��������� 5432
-- connect_database - ��� ���� ������ ��� ���������� ����������� � ��������
-- username - ������������ ��� ����������� � �������� ���� ������
-- password - ������ ��� ������������ 
-- description - �������� �������� ���� ������									
CREATE OR REPLACE FUNCTION cluster_add(IN connect_host varchar, 
									IN connect_database varchar, 
									IN username varchar, 
									IN password varchar, 
									IN description text) RETURNS bigint
AS $$
DECLARE
    ID    bigint;
BEGIN
	SELECT INTO ID cluster_add( connect_host, 
						5432,
						connect_database,
						username,
						password,
						description);
			
	RETURN ID;
				
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add(IN connect_host varchar, 
									IN connect_database varchar, 
									IN username varchar, 
									IN password varchar, 
									IN description text) IS '���������� ��������.';

-- ���������� �������� ��� ������
-- connect_host - ���� �������� ���� ������
-- connect_port - �� ��������� 5432
-- connect_database - �� ��������� postgres
-- username - ������������ ��� ����������� � �������� ���� ������
-- password - ������ ��� ������������ 
-- description - �������� �������� ���� ������									
CREATE OR REPLACE FUNCTION cluster_add(IN connect_host varchar, 
									IN username varchar, 
									IN password varchar, 									
									IN description text) RETURNS bigint
AS $$
DECLARE
    ID    bigint;
BEGIN
	SELECT INTO ID cluster_add(	connect_host, 
						'postgres',
						username,
						password,
						description);
			
	RETURN ID;
				
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add(IN connect_host varchar, 
									IN username varchar, 
									IN password varchar, 									
									IN description text) IS '���������� ��������.';
									

-- ���������� �������� ��� ������
-- connect_host - �� ��������� - ������� ���� localhost
-- connect_port - �� ��������� - ���� 5432
-- connect_database - �� ��������� - postgres
-- username - �� ��������� - postgres
-- password - ������ ������ 
-- description - 'Current cluster'
CREATE OR REPLACE FUNCTION cluster_add() RETURNS bigint
AS $$
DECLARE
    ID    bigint;
BEGIN
	SELECT INTO ID cluster_add(	'localhost', 
						'postgres',
						'postgres',
						'',
						'Current cluster`');
			
	RETURN ID;
				
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add() IS '���������� ��������.';
