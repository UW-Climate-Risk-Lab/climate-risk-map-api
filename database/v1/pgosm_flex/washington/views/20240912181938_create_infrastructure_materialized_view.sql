-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.infrastructure;
CREATE MATERIALIZED VIEW osm.infrastructure AS
SELECT
    i.osm_id,
    i.osm_type,
    i.osm_subtype,
    ST_GeometryType(i.geom) AS geom_type,
    i.geom,
    t.tags
FROM osm.infrastructure_point i
JOIN osm.tags t ON i.osm_id = t.osm_id
UNION ALL
SELECT
    i.osm_id,
    i.osm_type,
    i.osm_subtype,
    ST_GeometryType(i.geom) AS geom_type,
    i.geom,
    t.tags
FROM osm.infrastructure_line i
JOIN osm.tags t ON i.osm_id = t.osm_id
UNION ALL
SELECT
    i.osm_id,
    i.osm_type,
    i.osm_subtype,
    ST_GeometryType(i.geom) AS geom_type,
    i.geom,
    t.tags
FROM osm.infrastructure_polygon i
JOIN osm.tags t ON i.osm_id = t.osm_id;

CREATE INDEX infrastructure_idx_osm_id ON osm.infrastructure (osm_id);
CREATE INDEX infrastructure_idx_geom ON osm.infrastructure USING GIST (geom);
CREATE INDEX infrastructure_idx_osm_type ON osm.infrastructure (osm_type);
CREATE INDEX infrastructure_idx_osm_subtype ON osm.infrastructure (osm_subtype);
CREATE INDEX infrastructure_idx_osm_type_subtype ON osm.infrastructure (osm_type, osm_subtype);
CREATE INDEX infrastructure_idx_geom_type ON osm.infrastructure (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.infrastructure TO osm_ro_user;
GRANT SELECT ON osm.infrastructure TO climate_user;