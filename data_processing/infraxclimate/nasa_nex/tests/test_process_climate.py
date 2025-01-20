import pytest
import xarray as xr
import numpy as np
from unittest.mock import patch, MagicMock
from pathlib import Path
import os

from ..process_climate import decade_month_calc, climate_calc, main


# Test for the decade_month_calc function
def test_decade_month_calc():
    # Create a sample dataset with monthly data over 20 years (2000-2019)
    times = xr.cftime_range(start="2000-01-01", periods=240, freq="MS")
    data = np.random.rand(len(times))
    ds = xr.Dataset({"variable": ("time", data)}, coords={"time": times})

    # Run the decade_month_calc function
    result = decade_month_calc(ds, time_dim="time")

    # Assert that 'decade_month' is now a dimension
    assert "decade_month" in result.dims

    # Expecting 24 unique decade_month combinations (2 decades * 12 months)
    assert len(result["decade_month"]) == 24

    # Check that the mean calculation is correct for a sample decade_month
    for dm in result["decade_month"].values:
        decade, month = map(int, dm.split("-"))
        indices = ((ds["time.year"] // 10) * 10 == decade) & (ds["time.month"] == month)
        expected_mean = ds["variable"].sel(time=indices).mean().values
        actual_mean = result["variable"].sel(decade_month=dm).values
        np.testing.assert_almost_equal(actual_mean, expected_mean)


# Test for the climate_calc function
def test_climate_calc():
    # Create a sample dataset
    times = xr.cftime_range(start="2000-01-01", periods=240, freq="MS")
    data = np.random.rand(len(times))
    ds = xr.Dataset({"variable": ("time", data)}, coords={"time": times})

    # Test with a valid time aggregation method
    result = climate_calc(ds, time_dim="time", time_agg_method="decade_month")
    assert "decade_month" in result.dims

    # Test with an invalid time aggregation method
    with pytest.raises(ValueError) as excinfo:
        climate_calc(ds, time_dim="time", time_agg_method="invalid_method")
    assert "invalid_method time aggregation method not implemented" in str(
        excinfo.value
    )


# Test for the main function
def test_main(tmp_path):
    # Create a sample dataset with longitude and latitude
    times = xr.cftime_range(start="2000-01-01", periods=240, freq="MS")
    lon = np.linspace(0, 359, 10)
    lat = np.linspace(-90, 90, 10)

    data = np.random.rand(len(lat), len(lon), len(times))
    ds = xr.Dataset(
        data_vars=dict(variable=(["lat", "lon", "time"], data)),
        coords={"time": times, "lat": lat, "lon": lon},
        attrs={"key": "value"},
    )

    # Define parameters for the main function
    file_directory = str(tmp_path)
    xarray_engine = "h5netcdf"
    climate_variable = "variable"
    crs = "EPSG:4326"
    x_dim = "lon"
    y_dim = "lat"
    convert_360_lon = True
    bbox = None
    time_dim = "time"
    climatology_mean_method = "decade_month"
    derived_metadata_key = "derived"

    # Mock the utils.create_metadata function
    with patch("scenariomip.process_climate.read_data") as read_data:
        read_data.return_value = ds

        # Run the main function
        ds_result, metadata = main(
            file_directory=file_directory,
            xarray_engine=xarray_engine,
            climate_variable=climate_variable,
            crs=crs,
            x_dim=x_dim,
            y_dim=y_dim,
            convert_360_lon=convert_360_lon,
            bbox=bbox,
            time_dim=time_dim,
            climatology_mean_method=climatology_mean_method,
            derived_metadata_key=derived_metadata_key,
        )

    # Assert that the result is as expected
    assert isinstance(ds_result, xr.Dataset)
    assert metadata == {
        "key": "value",
        "variable": {},
        "derived": {
            "max_climate_variable_value": ds_result["variable"].max(),
            "min_climate_variable_value": ds_result["variable"].min(),
        },
    }
    assert climate_variable in ds_result.data_vars
    assert "decade_month" in ds_result.dims
    assert (ds_result[x_dim] >= -180).all() and (ds_result[x_dim] <= 180).all()
