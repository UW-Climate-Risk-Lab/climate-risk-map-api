import json
import uuid
from typing import Any, Dict, List
import logging

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from geojson_pydantic import FeatureCollection
from geojson_pydantic.features import Feature
from geojson_pydantic.geometries import Polygon
from fastapi import HTTPException

from . import schemas

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SSM = boto3.client("ssm")

def create_bbox(bboxes: List[schemas.BoundingBox]) -> FeatureCollection:
    """Creates GeoJSON spec. object from list of Bounding Boxes

    Args:
        bboxes (List[schemas.BoundingBox]): List of BoundingBox objects (see schemas.py)

    Returns:
        FeatureCollection: GeoJSON spec object
    """
    features = []

    for bbox in bboxes:
        # Create a Polygon geometry from bounding box
        polygon = Polygon.from_bounds(**bbox.model_dump())

        # Create a Feature with the Polygon geometry and empty properties
        feature = Feature(geometry=polygon, properties={}, type="Feature")

        features.append(feature)

    # Create and return the FeatureCollection
    return FeatureCollection(features=features, type="FeatureCollection")


def clean_geojson_data(raw_geojson: Dict[str, Any]) -> Dict[str, Any]:
    """Condense feature property fields to avoid duplicate features in return data

    We condense city and county since some features can span multiple (i.e long power transmission lines)
    Args:
        raw_geojson (Dict[str, Any]): Output of the query

    Returns:
        Dict[str, Any]: GeoJSON data with condensed climate and city fields
    """
    features = raw_geojson.get("features", [])
    aggregated_features = {}
    present_osm_ids = set()

    for feature in features:
        properties = feature.get("properties", {})
        osm_id = properties.get("osm_id")

        if osm_id is None:
            continue  # Skip features without an osm_id

        # Initialize the aggregated feature if it doesn't exist
        if osm_id not in present_osm_ids:
            # Deep copy to avoid mutating the original feature
            aggregated_features[osm_id] = {
                "type": feature.get("type"),
                "geometry": feature.get("geometry"),
                "properties": {
                    k: v
                    for k, v in properties.items()
                    if k
                    not in [
                        "geometry_wkt",
                        "latitude",
                        "longitude",
                        "county",
                        "city",
                    ]
                },
            }
            aggregated_features[osm_id]["properties"]["city"] = []
            aggregated_features[osm_id]["properties"]["county"] = []

            present_osm_ids.add(osm_id)

        city = properties.get("city")
        county = properties.get("county")

        if city not in aggregated_features[osm_id]["properties"]["city"]:
            aggregated_features[osm_id]["properties"]["city"].append(city)

        if county not in aggregated_features[osm_id]["properties"]["county"]:
            aggregated_features[osm_id]["properties"]["county"].append(county)

    # Convert the aggregated features back into a FeatureCollection
    new_features = list(aggregated_features.values())

    return {"type": "FeatureCollection", "features": new_features}


def upload_to_s3_and_get_presigned_url(
    bucket_name: str, prefix: str, data: dict, expiration: int = 3600
) -> str:
    """
    Uploads data to S3 and returns a presigned URL.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The name of the S3 prefix.
        data (dict): The data to upload.
        expiration (int): Time in seconds for the presigned URL to remain valid.

    Returns:
        str: The presigned URL.
    """
    s3_client = boto3.client("s3")
    object_key = prefix + f"{uuid.uuid4()}.geojson"

    try:
        # Upload the data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(data),
            ContentType="application/json",
        )

        # Generate a presigned URL
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration,
        )

        return presigned_url
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Error uploading to S3. Please contact us!"
        )

def check_data_size(data: str, threshold: float) -> bool:
    """Checks if the size of the str (in MB) is greater than the threshold

    Args:
        data (str): String data (commonly used will be the output of json.dumps())
        threshold (float): Threshold size in megabytes

    Returns:
        bool: True if over threshold
    """

    # Calculate the size of the serialized JSON string in bytes
    size_in_bytes = len(data.encode("utf-8"))

    # Convert the size from bytes to megabytes (MB)
    size_in_mb = size_in_bytes / (1024 * 1024)

    if size_in_mb > threshold:
        return True
    else:
        return False

def get_parameter(name):
    return SSM.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']