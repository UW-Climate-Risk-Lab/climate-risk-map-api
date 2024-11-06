-- This migration script creates a climate schema
-- This schema holds climate information that has been joined with osm features

CREATE ROLE climate_user WITH LOGIN PASSWORD 'mysecretpassword';

CREATE SCHEMA climate;

ALTER SCHEMA climate OWNER TO climate_user;

GRANT CONNECT ON DATABASE pgosm_flex_washington TO climate_user;

SET ROLE climate_user;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA climate TO climate_user;
GRANT CREATE ON SCHEMA climate TO climate_user;

-- Grant read and write privileges (SELECT, INSERT, UPDATE, DELETE) on all tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA climate TO climate_user;

-- Grant read and write privileges (USAGE, UPDATE) on sequences (for SERIAL columns)
GRANT USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA climate TO climate_user;

-- Grant read-only access to osm_ro_user
GRANT USAGE ON SCHEMA climate TO osm_ro_user;
GRANT SELECT ON ALL TABLES IN SCHEMA climate TO osm_ro_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA climate GRANT SELECT ON TABLES TO osm_ro_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA climate
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO climate_user;

-- Set default privileges for future sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA climate
    GRANT USAGE, UPDATE ON SEQUENCES TO climate_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA climate
    GRANT SELECT ON TABLES TO osm_ro_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA climate
    GRANT USAGE ON SEQUENCES TO osm_ro_user;

-- Us pgosm_flex role to grant table read permission on osm schema to climate user
-- Needed to allow climate_user to query infrastructure data for intersection computation
SET ROLE pgosm_flex;
GRANT USAGE ON SCHEMA osm TO climate_user;
GRANT SELECT ON ALL TABLES IN SCHEMA osm TO climate_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA osm GRANT SELECT ON TABLES TO climate_user;

SET ROLE climate_user;

-- create dimension table to hold scenario map variable information
CREATE TABLE climate.scenariomip_variables (
    id SERIAL PRIMARY KEY,
    variable TEXT NOT NULL,
    ssp INT NOT NULL,
    metadata JSONB
);
CREATE UNIQUE INDEX idx_unique_scenariomip_variable
    ON climate.scenariomip_variables (variable, ssp);

CREATE INDEX idx_scenariomip_variables_on_ssp ON climate.scenariomip_variables (ssp);

-- Create a table to hold scenariomip results
CREATE TABLE climate.scenariomip (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    month INT NOT NULL,
    decade INT NOT NULL,
    variable_id INT NOT NULL,
    value FLOAT NOT NULL,

    CONSTRAINT fk_variable_id FOREIGN KEY (variable_id)
        REFERENCES climate.scenariomip_variables (id)
);

-- Unique index to constrain possible values for a given feature (osm_id)
CREATE UNIQUE INDEX idx_unique_climate_record
    ON climate.scenariomip (osm_id, month, decade, variable_id);

-- Create an indexes for better join and filter performance, 
CREATE INDEX idx_scenariomip_on_osm_id ON climate.scenariomip (osm_id);
CREATE INDEX idx_scenariomip_on_month ON climate.scenariomip (month);
CREATE INDEX idx_scenariomip_on_decade ON climate.scenariomip (decade);
CREATE INDEX idx_scenariomip_on_variable_id ON climate.scenariomip (variable_id);