CREATE TABLE baselines (
    bl_id SERIAL PRIMARY KEY,
    bl_name varchar (25) UNIQUE,
    keep_until timestamp (0) with time zone
);
COMMENT ON TABLE baselines IS 'Baselines list';
