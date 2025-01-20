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
        ("s3://bucket/model1", "historical", ["s3://bucket/model1/historical"], True),
        # ssp is "585", and path exists
        ("s3://bucket/model1", "585", ["s3://bucket/model1/ssp585"], True),
        # ssp is "245" but path doesn't exist
        ("s3://bucket/model1", "245", [], False),
        # ssp is "historical" but path doesn't exist
        ("s3://bucket/model1", "historical", [], False),
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


# ------------------------------------------------------------------------------
# load_data
# ------------------------------------------------------------------------------
@pytest.mark.parametrize("bbox", [
    None,
    {"min_lon": -180, "min_lat": -90, "max_lon": 180, "max_lat": 90},
    {"min_lon": 9, "min_lat": -1, "max_lon": 12, "max_lat": 2},
])
def test_load_data(mock_s3fs, bbox):
    """
    Test load_data with a mock s3fs that pretends to find certain model directories / Zarr stores.
    We'll also mock xarray.open_mfdataset to return a synthetic dataset.

    To PASS validate_model_years, we must list *all* 2015..2100 years for "modelA."
    "modelB" is missing ssp585, so we skip it.
    """
    # The path for modelA:
    modelA_prefix = "s3://bucket/prefix/modelA/ssp585"
    all_future_uris = mock_all_future_stores(modelA_prefix, var="tas_day")

    mock_s3fs.glob.side_effect = lambda pattern: {
        "s3://bucket/prefix/*": [
            "s3://bucket/prefix/modelA/",
            "s3://bucket/prefix/modelB/",
        ],
        # For modelA, we have ssp585 with ALL future years
        "s3://bucket/prefix/modelA/ssp585/*/tas_day_*.zarr": all_future_uris,
        # For modelB, let's say we have no ssp585 => skip
        "s3://bucket/prefix/modelB/ssp585/*/tas_day_*.zarr": [],
    }.get(pattern, [])

    # Mock fs.exists() to ensure modelA's ssp585 is found, but modelB's is not
    def _mock_exists(path):
        # The code checks either:
        #   s3://bucket/prefix/modelA/ssp585
        # or
        #   s3://bucket/prefix/modelB/ssp585
        # Return True only for modelA
        if path == "s3://bucket/prefix/modelA/ssp585":
            return True
        return False

    mock_s3fs.exists.side_effect = _mock_exists

    # Synthetic dataset to be returned from open_mfdataset
    # We can have fewer time steps than 86 years x 12 months. 
    # The code only checks path-based years, not actual time coverage in the data.
    synthetic_ds = xr.Dataset(
        {
            "tas": (("time", "x", "y"), np.random.rand(12, 2, 2)),
        },
        coords={
            "time": xr.cftime_range(start="2015-01-01", periods=12, freq="MS"),
            "x": [10, 11],
            "y": [0, 1],
        },
    )

    with patch("process_climate.xr.open_mfdataset", return_value=synthetic_ds) as mock_open:
        da = load_data(
            s3_bucket="bucket",
            s3_prefix="prefix",
            ssp="585",
            climate_variable="tas",
            bbox=bbox,
        )

    # Only modelA should be loaded, because modelB is missing ssp585
    assert "model" in da.coords
    assert len(da["model"]) == 1  # Only modelA
    assert da["model"].values[0] == "modelA"

    # "time" was processed by decade_month_calc => replaced with "decade_month"
    # The synthetic dataset has 12 monthly points => 12 unique decade_month combos
    assert "decade_month" in da.coords
    assert len(da["decade_month"]) == 12

    # Confirm open_mfdataset was called once with the entire range of future zarr stores
    mock_open.assert_called_once()
    zarr_uris = mock_open.call_args[0][0]
    assert len(zarr_uris) == len(range(2015, 2101))  # 86 years
    # Spot check a few URIs
    assert zarr_uris[0].endswith("tas_day_2015.zarr")
    assert zarr_uris[-1].endswith("tas_day_2100.zarr")


# ------------------------------------------------------------------------------
# main (integration test)
# ------------------------------------------------------------------------------
def test_main(mock_s3fs):
    """
    Test the main function that orchestrates the entire process.
    We'll mock load_data-related S3 calls, as well as open_mfdataset.
    """
    # We'll pretend there's exactly 1 model "modelC" with a valid path + all future years
    mock_s3fs.glob.side_effect = lambda pattern: [
        "s3://bucket/prefix/modelC/",
    ]
    mock_s3fs.exists.side_effect = lambda path: path.endswith("ssp585")

    modelC_prefix = "s3://bucket/prefix/modelC/ssp585"
    all_future_uris = mock_all_future_stores(modelC_prefix, var="tas_day")

    # We'll refine the behavior if we see a pattern for modelC
    def custom_glob(pattern):
        if pattern == "s3://bucket/prefix/modelC/ssp585/*/tas_day_*.zarr":
            return all_future_uris
        return []
    mock_s3fs.glob.side_effect = custom_glob

    with patch("process_climate.xr.open_mfdataset") as mock_open:
        synthetic_ds = xr.Dataset(
            {
                "tas": (("time", "x", "y"), np.random.rand(12, 2, 2)),
            },
            coords={
                "time": xr.cftime_range(start="2015-01-01", periods=12, freq="MS"),
                "x": [10, 11],
                "y": [0, 1],
            },
        )
        mock_open.return_value = synthetic_ds

        # Call main
        bbox = {"min_lon": 9, "min_lat": -1, "max_lon": 12, "max_lat": 2}
        ds_result = main(
            ssp="585",
            s3_bucket="bucket",
            s3_prefix="prefix",
            climate_variable="tas",
            crs="4326",
            bbox=bbox,
        )

    # ds_result is the final dataset from reduce_model_stats
    # with rioxarray attributes set
    for varname in [
        "value_mean",
        "value_median",
        "value_stddev",
        "value_min",
        "value_max",
        "value_q1",
        "value_q3",
    ]:
        assert varname in ds_result

    # Confirm the CRS was set
    # (Your code uses rioxarray's write_crs and sets "EPSG:4326".)
    # ds_result.rio.crs should match
    assert ds_result.rio.crs is not None
    assert ds_result.rio.crs.to_string() == "4326"

    # Confirm chunking / shape: after reduce_model_stats, 'model' dimension is removed
    assert "model" not in ds_result.dims, "model dimension should be aggregated away."
    assert "decade_month" in ds_result.dims
    assert ds_result["decade_month"].size == 12
    # Confirm the x and y dimensions exist
    assert ds_result[constants.X_DIM].size == 2
    assert ds_result[constants.Y_DIM].size == 2

    # Check that open_mfdataset was called with all future years
    mock_open.assert_called_once()
    zarr_uris = mock_open.call_args[0][0]
    assert len(zarr_uris) == len(range(2015, 2101)), "Should have one .zarr per year 2015-2100."
    assert zarr_uris[0].endswith("tas_day_2015.zarr")
    assert zarr_uris[-1].endswith("tas_day_2100.zarr")