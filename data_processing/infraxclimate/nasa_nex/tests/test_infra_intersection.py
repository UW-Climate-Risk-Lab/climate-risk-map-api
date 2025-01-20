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

from ..infra_intersection import (
    zonal_aggregation,
    create_pgosm_flex_query,
    ID_COLUMN,
    GEOMETRY_COLUMN,
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
                [10.0,   20.0,   30.0,    40.0,     50.0],
                [100.0,  200.0,  300.0,   400.0,    500.0],
                [1000.0, 2000.0, 3000.0,  4000.0,   5000.0],
                [8.0,    9.0,    10.0,    11.0,     12.0],
                [13.0,   14.0,   15.0,    16.0,     17.0],
            ]
        ]
    )
    times = ["2020-01"]
    x = np.array([0, 1, 2, 3, 4])
    y = np.array([0, 1, 2, 3, 4])
    dims = ["decade_month", "y", "x"]
    ds = xr.Dataset(
        data_vars={"value_mean": (dims, data),
                    "value_median": (dims, data),
                    "value_stddev": (dims, data),
                    "value_min": (dims, data),
                    "value_max": (dims, data),
                    "value_q1": (dims, data),
                    "value_q3": (dims, data)},
        coords={"decade_month": times, "y": y, "x": x}
    )
    return ds


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
            "value_mean": [200.0, 17.0, 4000.0, 805.0],
            "value_median": [200.0, 17.0, 4000.0, 805.0],
            "value_stddev": [200.0, 17.0, 4000.0, 805.0],
            "value_min": [200.0, 17.0, 4000.0, 10.0],
            "value_max": [200.0, 17.0, 4000.0, 3000.0],
            "value_q1": [200.0, 17.0, 4000.0, 10.0],
            "value_q3": [200.0, 17.0, 4000.0, 3000.0],
            "decade": [2020, 2020, 2020, 2020],
            "month": [1, 1, 1, 1],
        }
    )

    # Call the function
    df = zonal_aggregation(
        climate=sample_climate_data,
        infra=sample_infra_data,
        zonal_agg_method="max",
        x_dim="x",
        y_dim="y",
    )

    assert_frame_equal(df.sort_values(by="osm_id").reset_index(drop=True), expected_df)


def test_zonal_aggregation_mean(sample_climate_data, sample_infra_data):

    expected_df = pd.DataFrame(
        data={
            "osm_id": [1, 2, 3, 4],
            "value_mean": [200.0, 17.0, 1755.25, 805.0],
            "value_median": [200.0, 17.0, 1755.25, 805.0],
            "value_stddev": [200.0, 17.0, 1755.25, 805.0],
            "value_min": [200.0, 17.0, 1755.25, 10.0],
            "value_max": [200.0, 17.0, 1755.25, 3000.0],
            "value_q1": [200.0, 17.0, 1755.25, 10.0],
            "value_q3": [200.0, 17.0, 1755.25, 3000.0],
            "decade": [2020, 2020, 2020, 2020],
            "month": [1, 1, 1, 1],
        }
    )

    # Call the function
    df = zonal_aggregation(
        climate=sample_climate_data,
        infra=sample_infra_data,
        zonal_agg_method="mean",
        x_dim="x",
        y_dim="y",
    )

    # Check that the DataFrame contains expected data
    assert_frame_equal(df.sort_values(by="osm_id").reset_index(drop=True), expected_df)
