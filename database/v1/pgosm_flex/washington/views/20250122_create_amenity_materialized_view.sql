-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.amenity;
CREATE MATERIALIZED VIEW osm.amenity AS
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.amenity_point a
JOIN osm.tags t ON a.osm_id = t.osm_id
UNION ALL
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.amenity_line a
JOIN osm.tags t ON a.osm_id = t.osm_id
UNION ALL
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.amenity_polygon a
JOIN osm.tags t ON a.osm_id = t.osm_id;

CREATE INDEX amenity_idx_osm_id ON osm.amenity (osm_id);
CREATE INDEX amenity_idx_geom ON osm.amenity USING GIST (geom);
CREATE INDEX amenity_idx_osm_type ON osm.amenity (osm_type);
CREATE INDEX amenity_idx_osm_subtype ON osm.amenity (osm_subtype);
CREATE INDEX amenity_idx_osm_type_subtype ON osm.amenity (osm_type, osm_subtype);
CREATE INDEX amenity_idx_geom_type ON osm.amenity (geom_type);