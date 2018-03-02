CREATE OR REPLACE FUNCTION cluster_add(IN connect_host varchar, 
									IN connect_port integer, 
									IN connect_database varchar, 
									IN description text, 
									IN username varchar, 
									IN password varchar) RETURNS bigint
AS $$
DECLARE
    ID    bigint;
BEGIN
	INSERT INTO clusters( 	connect_host, 
						connect_port, 
						connect_database, 
						description, 
						username, 
						password) 
			VALUES (	connect_host, 
						connect_port,
						connect_database,
						description,
						username,
						password)
			RETURNING cluster_id 
			INTO ID;
			
	RETURN ID;
				
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add(IN connect_host varchar, 
									IN connect_port integer, 
									IN connect_database varchar, 
									IN description text, 
									IN username varchar, 
									IN password varchar) IS 'Добавление кластера.';
