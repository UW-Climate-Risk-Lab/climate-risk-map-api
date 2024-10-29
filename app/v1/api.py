from typing import Any, Dict, List, Optional, Tuple
import logging

from app import database
from fastapi import APIRouter, HTTPException
from geojson_pydantic import FeatureCollection
from psycopg2 import sql

import app.v1.config as config
from app.v1.query import GetDataQueryBuilder
from app.v1.schemas import GetDataInputParameters, GetGeoJsonOutput

router = APIRouter()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@router.get("/data/geojson/{category}/")
def get_geojson(
    category: str,
    osm_types: List[str],
    osm_subtypes: Optional[List[str]] = None,
    bbox: Optional[FeatureCollection] = None,
    county: bool = False,
    city: bool = False,
    epsg_code: int = 4326,
    geom_type: Optional[str] = None,
    climate_variable: Optional[str] = None,
    climate_ssp: Optional[int] = None,
    climate_month: Optional[List[int]] = None,
    climate_decade: Optional[List[int]] = None,
    climate_metadata: bool = False,
) -> GetGeoJsonOutput:
    
    try:
        input_params = GetDataInputParameters(
            category=category,
            osm_types=osm_types,
            osm_subtypes=osm_subtypes,
            bbox=bbox,
            county=county,
            city=city,
            epsg_code=epsg_code,
            geom_type=geom_type,
            climate_variable=climate_variable,
            climate_ssp=climate_ssp,
            climate_month=climate_month,
            climate_decade=climate_decade,
            climate_metadata=climate_metadata,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    
    query_builder = GetDataQueryBuilder(input_params)

    query = query_builder.build_query()
    params = query_builder.params

    result = database.execute_query(query=query, params=tuple(params))
    geojson = result[0][0]

    return geojson


@router.get("/climate-metadata/")
def get_climate_metadata(climate_variable: str, ssp: str) -> Dict:
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
