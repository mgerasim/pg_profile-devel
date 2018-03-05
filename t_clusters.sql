CREATE TABLE clusters (
    cluster_id SERIAL PRIMARY KEY,
    name varchar(255) NOT NULL,
    connect_username varchar (25) NOT NULL,
    connect_password varchar (25) NOT NULL,
    connect_host varchar (25) NOT NULL,
    connect_port integer NOT NULL DEFAULT 5432,
    connect_database varchar (25) NOT NULL DEFAULT 'postgres',
    is_started boolean NOT NULL DEFAULT true,
    CONSTRAINT cns_cluster_host_port UNIQUE (connect_host, connect_port)
);

CREATE UNIQUE INDEX inx_cluster_name on clusters (lower(name));

COMMENT ON TABLE clusters IS 'The list of clusters of databases on which removal of snapshot is executed';