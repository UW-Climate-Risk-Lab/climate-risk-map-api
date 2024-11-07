from geojson_pydantic import FeatureCollection
from pydantic import BaseModel, model_validator, field_validator
from typing import List, Optional, Tuple

from . import config

class BoundingBox(BaseModel):
    """
    x = longtiude
    y = latitude
    """
    xmax: float 
    xmin: float 
    ymax: float
    ymin: float

    @field_validator('xmax', 'xmin')
    def check_longitude(cls, v):
        if not -180 <= v <= 180:
            raise ValueError('Longitude must be between -180 and 180')
        return v

    @field_validator('ymax', 'ymin')
    def check_latitude(cls, v):
        if not -90 <= v <= 90:
            raise ValueError('Latitude must be between -90 and 90')
        return v

class GetGeoJsonOutput(BaseModel):
    """Checks output is a GeoJSON"""

    geojson: FeatureCollection

class GetDataInputParameters(BaseModel):
    """Used to validate input parameters

    osm_category (str): OSM Category to get data from.
    osm_types (List[str]): OSM Type to filter on.
    osm_subtypes (List[str]): OSM Subtypes to filter on.
    bbox (FeatureCollection): Bounding Box. GeoJSON Spec format. Used for filtering.
    epsg_code (int): Spatial reference ID, default is 4326 (Representing EPSG:4326).
    geom_type (str): If used, returns only features of the specified geom_type.
    climate_variable (str): Climate variable to filter on.
    climate_ssp (int): Climate SSP (Shared Socioeconomic Pathway) to filter on.
    climate_month (List[int]): List of months to filter on.
    climate_decade (List[int]): List of decades to filter on.

    """

    osm_category: str
    osm_types: List[str]
    osm_subtypes: Optional[List[str]] = None
    bbox: Optional[FeatureCollection] = None
    epsg_code: int = 4326
    geom_type: Optional[str] = None
    climate_variable: Optional[str] = None
    climate_ssp: Optional[int] = None
    climate_month: Optional[List[int]] = None
    climate_decade: Optional[List[int]] = None
    limit: Optional[int] = None

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
    
    @model_validator(mode="after")
    def check_available_categories(self):
        if self.osm_category not in config.OSM_AVAILABLE_CATEGORIES.keys():
            raise ValueError(f"{self.osm_category} is not available")
        return self
        
    @model_validator(mode="after")
    def set_osm_subtypes(self):
        if not config.OSM_AVAILABLE_CATEGORIES[self.osm_category]["has_subtypes"]:
            self.osm_subtypes = None
        return self