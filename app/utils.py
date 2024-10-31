from typing import List
from geojson_pydantic import FeatureCollection
from geojson_pydantic.geometries import Polygon
from geojson_pydantic.features import Feature
import app.v1.schemas as schemas



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

