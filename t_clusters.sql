CREATE TABLE clusters (
    cluster_id SERIAL PRIMARY KEY,
    name varchar(255) NOT NULL UNIQUE,
    connect_username varchar (25) NOT NULL,
    connect_password varchar (25) NOT NULL,
    connect_host varchar (25) NOT NULL,
    connect_port integer NOT NULL,
    connect_database varchar (25) NOT NULL,
    CONSTRAINT cns_cluster_host_port UNIQUE(connect_host, connect_port)
);
COMMENT ON TABLE clusters IS 'Список кластеров баз данных, по которым выполняется снятие снепшотов';