CREATE TABLE clusters (
	cluster_id SERIAL PRIMARY KEY,
	cluster_hash varchar(25) UNIQUE,
	host varchar (25) NOT NULL,
	port integer NOT NULL,
	description text,
	username varchar (25) NOT NULL,
	password varchar (25) NOT NULL,
    CONSTRAINT cns_cluster_host_port UNIQUE(host, port)
);
CREATE INDEX idx_cluster_hash ON clusters(cluster_hash);
COMMENT ON TABLE clusters IS 'Список кластеров баз данных, по которым выполняется снятие снепшотов';