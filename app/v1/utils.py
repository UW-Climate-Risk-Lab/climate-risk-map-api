from typing import List, Dict, Any
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
                        "county",
                        "city",
                        "climate_variable",
                        "ssp",
                        "month",
                        "decade",
                        "climate_exposure",
                    ]
                },
            }
            # Creates a "climate" property
            aggregated_features[osm_id]["properties"]["climate"] = {
                "variable": properties.get("climate_variable"),
                "ssp": properties.get("ssp"),
                "months": [],
                "decades": [],
                "exposures": [],
            }
            aggregated_features[osm_id]["properties"]["city"] = []
            aggregated_features[osm_id]["properties"]["county"] = []

            present_osm_ids.add(osm_id)

        # Append the climate data to the lists
        aggregated_features[osm_id]["properties"]["climate"]["months"].append(
            properties.get("month")
        )
        aggregated_features[osm_id]["properties"]["climate"]["decades"].append(
            properties.get("decade")
        )
        aggregated_features[osm_id]["properties"]["climate"]["exposures"].append(
            properties.get("climate_exposure")
        )

        city = properties.get("city")
        county = properties.get("county")

        if city not in aggregated_features[osm_id]["properties"]["city"]:
            aggregated_features[osm_id]["properties"]["city"].append(city)

        if county not in aggregated_features[osm_id]["properties"]["county"]:
            aggregated_features[osm_id]["properties"]["county"].append(county)

    # Convert the aggregated features back into a FeatureCollection
    new_features = list(aggregated_features.values())

    return {"type": "FeatureCollection", "features": new_features}
