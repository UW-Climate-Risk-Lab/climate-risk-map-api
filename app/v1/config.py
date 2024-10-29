OSM_AVAILABLE_CATEGORIES = {"infrastructure": {"has_subtypes": True}}

OSM_SCHEMA_NAME = "osm"
OSM_TABLE_TAGS = "tags"
OSM_COLUMN_GEOM = "geom"
OSM_TABLE_PLACES = "place_polygon"  # This table contains Administrative Boundary data (cities, counties, towns, etc...)
OSM_TABLE_PLACES_ADMIN_LEVELS = {"county": 6, "city": 8}

CLIMATE_SCHEMA_NAME = "climate"
SCENARIOMIP_TABLE = "scenariomip"
SCENARIOMIP_VARIABLE_TABLE = "scenariomip_variables"
CLIMATE_TABLE_ALIAS = "climate_data"
