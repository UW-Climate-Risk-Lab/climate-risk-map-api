-- Creation of osminfra database
CREATE DATABASE washington;

\connect washington;

CREATE EXTENSION postgis;

CREATE ROLE pgosm_flex WITH LOGIN PASSWORD 'mysecretpassword';

CREATE SCHEMA osm;

ALTER SCHEMA osm OWNER TO pgosm_flex;

GRANT CREATE ON DATABASE washington
    TO pgosm_flex;
GRANT CREATE ON SCHEMA public
    TO pgosm_flex;

-- Creates a read only user for queries
CREATE ROLE osm_ro_user WITH LOGIN PASSWORD 'mysecretpassword';
GRANT CONNECT ON DATABASE washington TO osm_ro_user;
GRANT USAGE ON SCHEMA osm to osm_ro_user;

CREATE ROLE climate_user WITH LOGIN PASSWORD 'mysecretpassword';

-- This section creates a climate schema
-- This schema holds climate information that has been joined with osm features

CREATE SCHEMA climate;

ALTER SCHEMA climate OWNER TO climate_user;

GRANT CONNECT ON DATABASE washington TO climate_user;

-- Us pgosm_flex role to grant future table read privileges
SET ROLE pgosm_flex;
ALTER DEFAULT PRIVILEGES IN SCHEMA osm GRANT SELECT ON TABLES TO osm_ro_user;


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
