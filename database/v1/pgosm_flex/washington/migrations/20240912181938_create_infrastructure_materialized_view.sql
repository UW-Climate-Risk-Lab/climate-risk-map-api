-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.infrastructure;
CREATE MATERIALIZED VIEW osm.infrastructure AS
SELECT
    osm_id,
    osm_type,
    osm_subtype,
    ST_GeometryType(geom) AS geom_type,
    geom
FROM osm.infrastructure_point
UNION ALL
SELECT
    osm_id,
    osm_type,
    osm_subtype,
    ST_GeometryType(geom) AS geom_type,
    geom
FROM osm.infrastructure_line
UNION ALL
SELECT
    osm_id,
    osm_type,
    osm_subtype,
    ST_GeometryType(geom) AS geom_type,
    geom
FROM osm.infrastructure_polygon;

CREATE INDEX idx_osm_id ON osm.infrastructure (osm_id);
CREATE INDEX idx_geom ON osm.infrastructure USING GIST (geom);
CREATE INDEX idx_osm_type ON osm.infrastructure (osm_type);
CREATE INDEX idx_osm_subtype ON osm.infrastructure (osm_subtype);
CREATE INDEX idx_osm_type_subtype ON osm.infrastructure (osm_type, osm_subtype);
CREATE INDEX idx_geom_type ON osm.infrastructure (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.infrastructure TO osm_ro_user;
GRANT SELECT ON osm.infrastructure TO climate_user;