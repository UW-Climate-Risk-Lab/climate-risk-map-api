from typing import Any, Dict, List, Optional, Tuple
import logging

from app import database
from fastapi import APIRouter, Depends
from geojson_pydantic import FeatureCollection
from pydantic import BaseModel, ValidationError, model_validator
from psycopg2 import sql

import app.v1.config as config
import app.v1.query as query

router = APIRouter()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class GetDataInput(BaseModel):
    """Used to validate input parameters

    category (str): OSM Category to get data from.
    osm_types (List[str]): OSM Type to filter on.
    osm_subtypes (List[str]): OSM Subtypes to filter on.
    bbox (FeatureCollection): A Dict in the GeoJSON Feature Collection format. Used for filtering.
    county (bool): If True, returns the county of the feature as a property.
    city (bool): If True, returns the city of the feature as a property.
    epsg_code (int): Spatial reference ID, default is 4326 (Representing EPSG:4326).
    geom_type (str): If used, returns only features of the specified geom_type.
    climate_variable (str): Climate variable to filter on.
    climate_ssp (int): Climate SSP (Shared Socioeconomic Pathway) to filter on.
    climate_month (List[int]): List of months to filter on.
    climate_decade (List[int]): List of decades to filter on.
    climate_metadata (bool): Returns metadata of climate variable as dict.

    """

    category: str
    osm_types: List[str]
    osm_subtypes: Optional[List[str]] = None
    bbox: Optional[FeatureCollection] = None
    county: bool = False
    city: bool = False
    epsg_code: int = 4326
    geom_type: Optional[str] = None
    climate_variable: Optional[str] = None
    climate_ssp: Optional[int] = None
    climate_month: Optional[List[int]] = None
    climate_decade: Optional[List[int]] = None
    climate_metadata: bool = False

    # Custom validator to check that if climate data is provided, all required fields are present
    @model_validator(mode="after")
    def check_climate_params(self):
        if any(
            param is not None
            for param in [
                self.climate_variable,
                self.climate_ssp,
                self.climate_month,
                self.climate_decade,
            ]
        ):
            if self.climate_variable is None:
                raise ValueError(
                    "climate_variable is required when requesting climate data"
                )
            if self.climate_ssp is None:
                raise ValueError("climate_ssp is required when requesting climate data")
            if self.climate_month is None:
                raise ValueError(
                    "climate_month is required when requesting climate data"
                )
            if self.climate_decade is None:
                raise ValueError(
                    "climate_decade is required when requesting climate data"
                )
        return self

class GetGeoJsonOutput(BaseModel):
    """Checks output is a GeoJSON"""

    geojson: FeatureCollection



@router.get("/data/{category}")
def get_data(params: GetDataInput = Depends()):
    category = params.category
    osm_types = params.osm_types
    osm_subtypes = params.osm_subtypes
    bbox = params.bbox
    county = params.county
    city = params.city
    epsg_code = params.epsg_code
    geom_type = params.geom_type
    climate_variable = params.climate_variable
    climate_ssp = params.climate_ssp
    climate_month = params.climate_month
    climate_decade = params.climate_decade
    climate_metadata = params.climate_metadata

    # Primary table will be a materialized view of the given category
    primary_table = category if category in config.OSM_AVAILABLE_CATEGORIES else None
    if primary_table is None:
        raise ValueError(f"The category {category} is not currently available!")

    primary_table_columns = self._get_table_columns(table_name=primary_table)

    # Some categories do not have osm_subtype (like the "places" category)
    if "osm_subtype" not in primary_table_columns:
        osm_subtypes = None

    # Params are added in order while creating SQL statements
    query_params = []

    # This builds a query to return a GeoJSON object
    # This method should always return a GeoJSON to the client
    geojson_statement = sql.SQL(
        """ 
    SELECT json_build_object(
        'type', 'FeatureCollection',
        'features', json_agg(ST_AsGeoJSON(geojson.*)::json))
        
        FROM (
    """
    )

    select_statement, query_params = self._create_select_statement(
        params=query_params,
        primary_table=primary_table,
        epsg_code=epsg_code,
        osm_subtypes=osm_subtypes,
        county=county,
        city=city,
        climate_variable=climate_variable,
        climate_ssp=climate_ssp,
        climate_decade=climate_decade,
        climate_month=climate_month,
        climate_metadata=climate_metadata,
    )

    from_statement = self._create_from_statement(primary_table=primary_table)

    join_statement, query_params = self._create_join_statement(
        primary_table=primary_table,
        params=query_params,
        county=county,
        city=city,
        climate_variable=climate_variable,
        climate_ssp=climate_ssp,
        climate_decade=climate_decade,
        climate_month=climate_month,
    )

    where_clause, query_params = self._create_where_clause(
        primary_table=primary_table,
        params=query_params,
        osm_types=osm_types,
        osm_subtypes=osm_subtypes,
        geom_type=geom_type,
        bbox=bbox,
        epsg_code=epsg_code,
    )

    query = sql.SQL(" ").join(
        [
            geojson_statement,
            select_statement,
            from_statement,
            join_statement,
            where_clause,
            sql.SQL(") AS geojson;"),
        ]
    )

    result = database.execute_query(query=query, params=tuple(query_params))
    geojson = result[0][0]
    try:
        GetGeoJsonOutput(geojson=geojson)
    except ValidationError as e:
        logger.error()

    return geojson

@router.get("/climate-metadata/")
def get_climate_metadata(self, climate_variable: str, ssp: str) -> Dict:
    """Returns climate metadata JSON blob for given climate_variable and ssp

    Args:
        climate_variable (str): climate variable name
        ssp (str): SSP number

    Returns:
        Dict: JSON blob of climate metadata
    """

    query = sql.SQL(
        "SELECT metadata FROM {schema}.{scenariomip_variable} WHERE variable = %s AND ssp = %s"
    ).format(
        schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
        scenariomip_variable=sql.Identifier(config.SCENARIOMIP_VARIABLE_TABLE),
    )

    result = database.execute_query(query=query, params=(climate_variable, ssp))

    return result[0][0]
