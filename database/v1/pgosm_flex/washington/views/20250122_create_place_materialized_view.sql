-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.place;
CREATE MATERIALIZED VIEW osm.place AS
SELECT
    p.osm_id,
    p.osm_type,
    p.boundary,
    p.admin_level,
    p.name,
    ST_GeometryType(p.geom) AS geom_type,
    p.geom,
    t.tags
FROM osm.place_point p
JOIN osm.tags t ON p.osm_id = t.osm_id
UNION ALL
SELECT
    p.osm_id,
    p.osm_type,
    p.boundary,
    p.admin_level,
    p.name,
    ST_GeometryType(p.geom) AS geom_type,
    p.geom,
    t.tags
FROM osm.place_line p
JOIN osm.tags t ON p.osm_id = t.osm_id
UNION ALL
SELECT
    p.osm_id,
    p.osm_type,
    p.boundary,
    p.admin_level,
    p.name,
    ST_GeometryType(p.geom) AS geom_type,
    p.geom,
    t.tags
FROM osm.place_polygon p
JOIN osm.tags t ON p.osm_id = t.osm_id;

CREATE INDEX place_idx_osm_id ON osm.place (osm_id);
CREATE INDEX place_idx_geom ON osm.place USING GIST (geom);
CREATE INDEX place_idx_osm_type ON osm.place (osm_type);
CREATE INDEX place_idx_osm_type_boundary ON osm.place (osm_type, boundary);
CREATE INDEX place_idx_osm_admin_level ON osm.place (admin_level);
CREATE INDEX place_idx_geom_type ON osm.place (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.place TO osm_ro_user;
GRANT SELECT ON osm.place TO climate_user;