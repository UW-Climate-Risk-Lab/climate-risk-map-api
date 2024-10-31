from typing import Any, Dict, List, Optional, Tuple
import logging
import json


from fastapi import APIRouter, HTTPException, Query
from geojson_pydantic import FeatureCollection
from psycopg2 import sql

import app.database as database
import app.v1.config as config
from app.v1.query import GetDataQueryBuilder
import app.v1.schemas as schemas
import app.utils as utils

router = APIRouter()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_gejson():
    pass

def get_csv():
    pass

@router.get("/data/{response_format}/{category}/{osm_type}/")
def get_data(
    response_format: str, # TODO: configure to allow CSV or Geojson
    category: str,
    osm_type: str,
    osm_subtypes: List[str] | None = Query(None),
    bbox: List[str] | None = Query(None),
    county: bool = False,
    city: bool = False,
    epsg_code: int = 4326,
    geom_type: str | None = None,
    climate_variable: str | None = None,
    climate_ssp: int | None = None,
    climate_month: List[int] | None = Query(None),
    climate_decade: List[int] | None = Query(None),
    climate_metadata: bool = False,
) -> Dict:

    # Convert to list to match schema
    # In the future users may be able to included multiple types,
    # restrict to single type now to limit data transfer cost
    osm_types = (osm_type,)

    # 
    if bbox:
        try:
            bbox_list = [schemas.BoundingBox(**json.loads(box)) for box in bbox]
        except json.JSONDecodeError:
            # User should input bbox(s) query parameter in this format
            input_format = '{"xmin": -126.0, "xmax": -119.0, "ymin": 46.1, "ymax": 47.2}'
            return {
                "error": f'Invalid bounding box JSON format. Example: bbox={input_format}'
            }
        except ValueError as e:
            return {
                "error": str(e)
            }
        
        try:
            bbox = utils.create_bbox(bbox_list)
        except Exception as e:
            print(str(e))

    try:
        input_params = schemas.GetDataInputParameters(
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
        print(input_params)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    query_builder = GetDataQueryBuilder(input_params)

    query = query_builder.build_query()
    params = query_builder.params

    result = database.execute_query(query=query, params=tuple(params))
    result = result[0][0]

    try:
        if result["features"] is None:
            result["features"] = list()
        else:
            pass
    except KeyError as e:
        logger.error("Get GeoJSON database response has no key 'features'")

    # Serialize the response data to JSON
    response_json = json.dumps(result)

    # Calculate the size of the serialized JSON string in bytes
    size_in_bytes = len(response_json.encode("utf-8"))

    # Convert the size from bytes to megabytes (MB)
    size_in_mb = size_in_bytes / (1024 * 1024)

    print(size_in_mb)

    try:
        schemas.GetGeoJsonOutput(geojson=result)
    except Exception as e:
        logger.error(f"Validation of GeoJSON return object schema failed for GET geojson: {str(e)}")
        raise HTTPException(status_code=500, detail="Return GeoJSON format failed validation. Please contact us!") 

    return result


@router.get("/climate-metadata/{climate_variable}/{ssp}")
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
