-- Добавление кластера баз данных
-- connect_host - Хост кластера базы данных
-- connect_port - Порт для подключения к кластеру базы данных
-- connect_database - Имя базы данных для начального подключения к кластеру
-- username - Пользователь для подключения к кластеру базе данных
-- password - Пароль для пользователя 
-- description - Описание кластера базы данных
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
									IN description text) IS 'Добавление кластера.';

-- Добавление кластера баз данных
-- connect_host - Хост кластера базы данных
-- connect_port - по умолчанию 5432
-- connect_database - Имя базы данных для начального подключения к кластеру
-- username - Пользователь для подключения к кластеру базе данных
-- password - Пароль для пользователя 
-- description - Описание кластера базы данных									
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
									IN description text) IS 'Добавление кластера.';

-- Добавление кластера баз данных
-- connect_host - Хост кластера базы данных
-- connect_port - По умолчанию 5432
-- connect_database - По умолчанию postgres
-- username - Пользователь для подключения к кластеру базе данных
-- password - Пароль для пользователя 
-- description - Описание кластера базы данных									
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
									IN description text) IS 'Добавление кластера.';
									

-- Добавление кластера баз данных
-- connect_host - По умолчанию - текущий хост localhost
-- connect_port - По умолчанию - порт 5432
-- connect_database - По умолчанию - postgres
-- username - По умолчанию - postgres
-- password - Пароль пустой 
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

COMMENT ON FUNCTION cluster_add() IS 'Добавление кластера.';
