from geojson_pydantic import FeatureCollection
from typing import List
import v1.schemas

def create_bbox(bboxs: List[v1.schemas.BoundingBox]) -> FeatureCollection:
    
    