-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.landuse;
CREATE MATERIALIZED VIEW osm.landuse AS
SELECT
    lpoint.osm_id,
    lpoint.osm_type,
    lpoint.name,
    ST_GeometryType(lpoint.geom) AS geom_type,
    lpoint.geom,
    tpoint.tags
FROM osm.landuse_point lpoint
JOIN osm.tags tpoint ON lpoint.osm_id = tpoint.osm_id
UNION ALL
SELECT
    lpolygon.osm_id,
    lpolygon.osm_type,
    lpolygon.name,
    ST_GeometryType(lpolygon.geom) AS geom_type,
    lpolygon.geom,
    tpolygon.tags
FROM osm.landuse_polygon lpolygon
JOIN osm.tags tpolygon ON lpolygon.osm_id = tpolygon.osm_id;

CREATE INDEX landuse_idx_osm_id ON osm.landuse (osm_id);
CREATE INDEX landuse_idx_geom ON osm.landuse USING GIST (geom);
CREATE INDEX landuse_idx_osm_type ON osm.landuse (osm_type);
CREATE INDEX landuse_idx_geom_type ON osm.landuse (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.landuse TO osm_ro_user;
GRANT SELECT ON osm.landuse TO climate_user;