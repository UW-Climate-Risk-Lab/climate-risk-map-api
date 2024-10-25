from fastapi import APIRouter
from geojson_pydantic import FeatureCollection
from pydantic import BaseModel, model_validator, ValidationError
from typing import List, Dict, Tuple, Optional, Any

data_router = APIRouter()

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

class GetDataOutput(BaseModel):
    """Checks output is a GeoJSON"""

    geojson: FeatureCollection

@data_router.get("/api/v1/data/")
def get_data():
    pass