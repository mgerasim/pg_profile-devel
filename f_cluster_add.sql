CREATE OR REPLACE FUNCTION cluster_add(IN _name varchar, 
                                       IN _connect_username varchar, 
                                       IN _connect_password varchar,
                                       IN _connect_host varchar, 
                                       IN _connect_port integer DEFAULT 5432, 
                                       IN _connect_database varchar DEFAULT 'postgres') RETURNS bigint 
SET search_path=public 
AS $$
DECLARE
    ID    bigint;
BEGIN
    INSERT INTO clusters(name, 
                         connect_username, 
                         connect_password,
                         connect_host, 
                         connect_port, 
                         connect_database) 
    VALUES (_name, 
            _connect_username, 
            _connect_password,
            _connect_host, 
            _connect_port, 
            _connect_database) 
    RETURNING cluster_id INTO ID;            
    RETURN ID;                
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cluster_add(IN _name varchar, 
                                IN _connect_username varchar, 
                                IN _connect_password varchar,
                                IN _connect_host varchar, 
                                IN _connect_port integer, 
                                IN _connect_database varchar) IS 'Р”РѕР±Р°РІР»РµРЅРёРµ РєР»Р°СЃС‚РµСЂР°.';

