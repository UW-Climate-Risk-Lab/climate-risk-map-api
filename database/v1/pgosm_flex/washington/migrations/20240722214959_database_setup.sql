-- Creation of osminfra database
CREATE DATABASE pgosm_flex_washington;

\connect pgosm_flex_washington;

CREATE EXTENSION postgis;

CREATE ROLE pgosm_flex WITH LOGIN PASSWORD 'mysecretpassword';

CREATE SCHEMA osm;

ALTER SCHEMA osm OWNER TO pgosm_flex;

GRANT CREATE ON DATABASE pgosm_flex_washington
    TO pgosm_flex;
GRANT CREATE ON SCHEMA public
    TO pgosm_flex;

-- Creates a read only user for queries
CREATE ROLE osm_ro_user WITH LOGIN PASSWORD 'mysecretpassword';
GRANT CONNECT ON DATABASE pgosm_flex_washington TO osm_ro_user;
GRANT USAGE ON SCHEMA osm to osm_ro_user;

-- Us pgosm_flex role to grant future table read privileges
SET ROLE pgosm_flex;
ALTER DEFAULT PRIVILEGES IN SCHEMA osm GRANT SELECT ON TABLES TO osm_ro_user;


