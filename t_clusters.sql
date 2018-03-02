CREATE TABLE clusters (
	cluster_id SERIAL PRIMARY KEY,
	connect_host varchar (25) NOT NULL,
	connect_port integer NOT NULL,
	connect_database varchar (25) NOT NULL,
	description text,
	username varchar (25) NOT NULL,
	password varchar (25) NOT NULL,
    CONSTRAINT cns_cluster_host_port UNIQUE(connect_host, connect_port)
);
COMMENT ON TABLE clusters IS 'Список кластеров баз данных, по которым выполняется снятие снепшотов';