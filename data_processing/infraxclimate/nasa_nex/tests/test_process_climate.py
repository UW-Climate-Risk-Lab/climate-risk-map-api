import pytest
import logging
import numpy as np
import xarray as xr
from unittest.mock import MagicMock, patch

# Adjust import paths as needed
from ..process_climate import (
    validate_model_ssp,
    validate_model_years,
    decade_month_calc,
    reduce_model_stats,
    load_data,
    main,
    HISTORICAL_YEARS,
    FUTURE_YEARS
)

import constants

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_s3fs():
    """
    Pytest fixture that returns a mock s3fs.S3FileSystem object.
    We can override .exists(), .glob(), etc.
    """
    with patch("process_climate.s3fs.S3FileSystem") as mock_fs_class:
        mock_fs = MagicMock()
        mock_fs_class.return_value = mock_fs
        yield mock_fs


# ------------------------------------------------------------------------------
# validate_model_ssp
# ------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "model_path, ssp, existing_paths, expected",
    [
        # ssp is "historical"
        ("s3://bucket/model1", "-999", ["s3://bucket/model1/historical"], True),
        # ssp is "585", and path exists
        ("s3://bucket/model1", "585", ["s3://bucket/model1/ssp585"], True),
        # ssp is "245" but path doesn't exist
        ("s3://bucket/model1", "245", [], False),
        # ssp is "historical" but path doesn't exist
        ("s3://bucket/model1", "-999", [], False),
    ],
)
def test_validate_model_ssp(mock_s3fs, model_path, ssp, existing_paths, expected):
    # Mock the fs.exists() to return True if the path is in our list of existing_paths
    mock_s3fs.exists.side_effect = lambda p: p in existing_paths
    
    result = validate_model_ssp(mock_s3fs, model_path, ssp)
    assert result == expected, f"Expected {expected} for model_path={model_path}, ssp={ssp}"


# ------------------------------------------------------------------------------
# validate_model_years
# ------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "zarr_stores, expected",
    [
        # Perfectly matches HISTORICAL_YEARS (1950 to 2014 inclusive)
        (
            [f"path_{y}.zarr" for y in range(1950, 2015)],
            True,
        ),
        # Perfectly matches FUTURE_YEARS (2015 to 2100 inclusive)
        (
            [f"path_{y}.zarr" for y in range(2015, 2101)],
            True,
        ),
        # Missing one year in historical
        (
            [f"path_{y}.zarr" for y in range(1950, 2015) if y != 1960],
            False,
        ),
        # Contains extra year not in historical or future
        (
            [f"path_{y}.zarr" for y in range(1950, 2016)],
            False,
        ),
        # Mixed historical + future in same list
        (
            [f"path_{y}.zarr" for y in range(1950, 2101)],
            False,
        ),
    ],
)
def test_validate_model_years(zarr_stores, expected):
    """
    We only care if the entire set is exactly HISTORICAL_YEARS or FUTURE_YEARS.
    """
    # fs is not used directly, so pass None
    result = validate_model_years(None, zarr_stores)
    assert result == expected


# ------------------------------------------------------------------------------
# decade_month_calc
# ------------------------------------------------------------------------------
def test_decade_month_calc():
    """
    Test that decade_month_calc groups correctly by decade and month.
    We'll create a small synthetic dataset with time dimension spanning 1950-1959 monthly.
    That is 120 months. The code lumps them by decade (1950) + month => 12 unique groups.
    """
    time = xr.cftime_range(start="1950-01-01", end="1959-12-01", freq="MS", calendar="gregorian")
    data = np.random.rand(len(time), 2, 2)  # shape=(120, 2, 2)
    ds = xr.Dataset(
        {
            "temp": (["time", "lat", "lon"], data),
        },
        coords={
            "time": time,
            "lat": [0, 1],
            "lon": [10, 11],
        },
    )

    # Apply the decade_month_calc
    result = decade_month_calc(ds, time_dim="time")

    # Because 1950 through 1959 is still the "1950s" => decade=1950
    # We get 12 unique decade-month combos, one per month of the year
    # even though the time series is 10 years long.
    assert "decade_month" in result.coords
    # The number of unique groups is 12:
    assert len(result["decade_month"]) == 12


# ------------------------------------------------------------------------------
# reduce_model_stats
# ------------------------------------------------------------------------------
def test_reduce_model_stats():
    """
    Test that reduce_model_stats returns a Dataset with correct stats.
    """
    # Create a DataArray with dimension "model"
    # Let's have 3 models, 4 time steps, 2x2 lat/lon
    data = np.array(
        [
            [[[1, 2], [3, 4]]],  # Model A, shape = (1, 2, 2)
            [[[2, 4], [2, 2]]],  # Model B
            [[[5, 5], [5, 5]]],  # Model C
        ]
    ).astype(float)
    # Expand into time dimension of size 4 if we want (just repeat data in time axis)
    data = np.repeat(data, 4, axis=1)  # Now shape is (3, 4, 2, 2)
    da = xr.DataArray(
        data,
        dims=("model", "time", "lat", "lon"),
        coords={
            "model": ["ModelA", "ModelB", "ModelC"],
            "time": range(4),
            "lat": [0, 1],
            "lon": [10, 11],
        },
        name="temperature",
    )

    # Provide a quick ensemble_members attribute
    da.attrs["ensemble_members"] = list(da.model.values)

    # Run reduce
    result_ds = reduce_model_stats(da)

    # Check that the output is a Dataset with the expected variables
    for varname in [
        "value_mean",
        "value_median",
        "value_stddev",
        "value_min",
        "value_max",
        "value_q1",
        "value_q3",
    ]:
        assert varname in result_ds

    # Check sample_size
    assert result_ds.attrs["sample_size"] == 3  # we had 3 models
    # Check shape: (time=4, lat=2, lon=2)
    assert result_ds["value_mean"].shape == (4, 2, 2)

    # Spot check the mean at time=0
    #   ModelA => [[1,2],[3,4]]
    #   ModelB => [[2,4],[2,2]]
    #   ModelC => [[5,5],[5,5]]
    # mean => elementwise:
    #    => [[(1+2+5)/3, (2+4+5)/3],
    #        [(3+2+5)/3, (4+2+5)/3]]
    #    => [[2.666..., 3.666...],[3.333..., 3.666...]]
    mean_values = result_ds["value_mean"].isel(time=0)
    np.testing.assert_almost_equal(mean_values.values, [[8/3, 11/3], [10/3, 11/3]], decimal=5)


# ------------------------------------------------------------------------------
# Helper for mocking ALL future years
# ------------------------------------------------------------------------------
def mock_all_future_stores(model_prefix, var="tas_day"):
    """
    Return a list of fully valid future Zarr paths for 2015..2100
    E.g.:
      s3://bucket/prefix/modelA/ssp585/2015/tas_day_2015.zarr
      s3://bucket/prefix/modelA/ssp585/2016/tas_day_2016.zarr
      ...
      s3://bucket/prefix/modelA/ssp585/2100/tas_day_2100.zarr
    """
    return [
        f"{model_prefix}/{year}/{var}_{year}.zarr" 
        for year in range(2015, 2101)
    ]