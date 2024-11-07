from typing import Dict, List
import logging
import json
import os

from fastapi import APIRouter, HTTPException, Query
from psycopg2 import sql

from .. import database

from . import config
from . import schemas
from . import utils

from .query import GetDataQueryBuilder

router = APIRouter()

S3_BUCKET = os.environ["S3_BUCKET"]
S3_BASE_PREFIX_USER_DOWNLOADS = os.environ["S3_BASE_PREFIX_USER_DOWNLOADS"]
DATA_SIZE_RETURN_LIMIT_MB=float(os.environ["DATA_SIZE_RETURN_LIMIT_MB"])

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@router.get("/data/{response_format}/{osm_category}/{osm_type}/{osm_subtype}/")
def get_data(
    response_format: str,  # TODO: configure to allow CSV or Geojson
    osm_category: str,
    osm_type: str,
    osm_subtype: str,
    bbox: List[str] | None = Query(None),
    epsg_code: int = 4326,
    geom_type: str | None = None,
    climate_variable: str | None = None,
    climate_ssp: int | None = None,
    climate_month: int | None = None,
    climate_decade: int | None = None,
    limit: int | None = None,
) -> Dict:

    # Public facing API will not allow users to input lists of types, months, and decades to limit data size
    # Convert to single element tuple after request since query builder can handle lists
    osm_types = (osm_type,)
    if osm_subtype:
        osm_subtypes = (osm_subtype,)
    if climate_month:
        climate_month = (climate_month,)
    if climate_decade:
        climate_decade = (climate_decade,)

    # TODO: Add CSV response format
    if response_format.lower() not in ["geojson"]:
        raise HTTPException(
            status_code=422, detail=f"{response_format} response format not supported"
        )

    if bbox:
        try:
            bbox_list = [schemas.BoundingBox(**json.loads(box)) for box in bbox]
        except json.JSONDecodeError:
            # User should input bbox(s) query parameter in this format
            input_format = (
                '{"xmin": -126.0, "xmax": -119.0, "ymin": 46.1, "ymax": 47.2}'
            )
            return {
                "error": f"Invalid bounding box JSON format. Example: bbox={input_format}"
            }
        except ValueError as e:
            return {"error": str(e)}

        try:
            bbox = utils.create_bbox(bbox_list)
        except Exception as e:
            logger.error(f"Error creating geojson from bounding box input: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="There was an error parsing provided bounding boxes",
            )

    try:
        input_params = schemas.GetDataInputParameters(
            osm_category=osm_category,
            osm_types=osm_types,
            osm_subtypes=osm_subtypes,
            bbox=bbox,
            epsg_code=epsg_code,
            geom_type=geom_type,
            climate_variable=climate_variable,
            climate_ssp=climate_ssp,
            climate_month=climate_month,
            climate_decade=climate_decade,
            limit=limit,
        )
        print(input_params)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    query, query_params = GetDataQueryBuilder(input_params).build_query()

    result = database.execute_query(query=query, params=query_params)
    result = result[0][0]

    try:
        if result["features"] is None:
            result["features"] = list()
        else:
            result = utils.clean_geojson_data(raw_geojson=result)
    except KeyError as e:
        logger.error("Get GeoJSON database response has no key 'features'")


    try:
        schemas.GetGeoJsonOutput(geojson=result)
    except Exception as e:
        logger.error(
            f"Validation of GeoJSON return object schema failed for GET geojson: {str(e)}"
        )
        raise HTTPException(
            status_code=500,
            detail="Return GeoJSON format failed validation. Please contact us!",
        )

    if utils.check_data_size(data=json.dumps(result), threshold=DATA_SIZE_RETURN_LIMIT_MB):

        presigned_url = utils.upload_to_s3_and_get_presigned_url(
            bucket_name=S3_BUCKET, prefix=S3_BASE_PREFIX_USER_DOWNLOADS, data=result
        )
        return {"presigned_url": presigned_url}

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
    result = result[0][0]

    return {"climate_variable": climate_variable,
            "ssp": ssp,
            "metadata": result}
