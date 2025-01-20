import pytest
import xarray as xr
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString
from shapely import wkt
import psycopg2
import psycopg2.sql as sql
from pandas.testing import assert_frame_equal

from nasa_nex.infra_intersection import (

    zonal_aggregation,
    create_pgosm_flex_query,
    ID_COLUMN,
    GEOMETRY_COLUMN,
    VALUE_COLUMN,
)

def test_create_pgosm_flex_query():
    osm_tables = ["infrastructure_point", "infrastructure_polygon"]
    osm_type = "power"
    crs = "4326"
    query, params = create_pgosm_flex_query(osm_tables, osm_type, crs)

    # Check that the query is of type sql.SQL
    assert isinstance(query, sql.Composed)

    # Check that the parameters are correct
    expected_params = (4326, osm_type, 4326, osm_type)
    assert params == expected_params

    # You can also check that the query contains expected identifiers
    for table in osm_tables:
        assert table in str(query)


@pytest.fixture
def sample_climate_data():
    # Create a sample climate DataArray
    data = np.array(
        [
            [
                [10., 20., 30., 40., 50.],
                [100., 200., 300., 400., 500.],
                [1000., 2000., 3000., 4000., 5000.],
                [8., 9., 10., 11., 12.],
                [13., 14., 15., 16., 17.],
            ]
        ]
    )
    times = ["2020-01"]
    x = np.array([0, 1, 2, 3, 4])
    y = np.array([0, 1, 2, 3, 4])
    da = xr.DataArray(data, coords=[("decade_month", times), ("y", y), ("x", x)])
    return da


@pytest.fixture
def sample_infra_data():
    # Create a sample GeoDataFrame with some geometries
    geometries = [
        Point(1, 1),
        Point(4, 4),
        Polygon([(2, 2), (2, 3), (3, 3), (3, 2)]),
        LineString([(0, 0), (1, 1), (2, 2), (2, 3)]),
    ]
    df = pd.DataFrame({ID_COLUMN: [1, 2, 3, 4], GEOMETRY_COLUMN: geometries})
    gdf = gpd.GeoDataFrame(df, geometry=GEOMETRY_COLUMN).set_index(ID_COLUMN)
    gdf = gdf.set_crs("EPSG:4326")
    return gdf



def test_zonal_aggregation_max(sample_climate_data, sample_infra_data):

    expected_df = pd.DataFrame(
        data={
            "osm_id": [1, 2, 3, 4],
            VALUE_COLUMN: [200., 17., 4000., 3000.],
            "decade": [2020, 2020, 2020, 2020],
            "month": [1, 1, 1, 1],
        }
    )


    # Call the function
    df = zonal_aggregation(
        climate=sample_climate_data,
        infra=sample_infra_data,
        zonal_agg_method="max",
        climatology_mean_method="decade_month",
        x_dim="x",
        y_dim="y",
        crs="4326"
    )

    
    assert_frame_equal(df.sort_values(by="osm_id").reset_index(drop=True), expected_df)

def test_zonal_aggregation_mean(sample_climate_data, sample_infra_data):

    expected_df = pd.DataFrame(
        data={
            "osm_id": [1, 2, 3, 4],
            VALUE_COLUMN: [200., 17., 1755.25, 805.],
            "decade": [2020, 2020, 2020, 2020],
            "month": [1, 1, 1, 1],
        }
    )

    # Call the function
    df = zonal_aggregation(
        climate=sample_climate_data,
        infra=sample_infra_data,
        zonal_agg_method="mean",
        climatology_mean_method="decade_month",
        x_dim="x",
        y_dim="y",
        crs="4326"
    )

    # Check that the DataFrame contains expected data
    assert_frame_equal(df.sort_values(by="osm_id").reset_index(drop=True), expected_df)
