import logging
from pathlib import Path
import re

import rioxarray
import xarray as xr
import fsspec
import s3fs

import constants

from typing import List, Set

logger = logging.getLogger(__name__)

# Add constants for validation. Models used must have all available scenarios and all available years
HISTORICAL_YEARS = set(range(1950, 2015))  # 1950-2014
FUTURE_YEARS = set(range(2015, 2101))  # 2015-2100


def validate_model_ssp(fs: s3fs.S3FileSystem, model_path: str, ssp: str) -> bool:
    """Check if model has required SSP. SSP may equal 'historical'"""

    if fs.exists(f"{model_path}/ssp{ssp}"):
        return True

    # Catches 'historical'
    if fs.exists(f"{model_path}/{ssp}"):
        return True
    
    return False


def validate_model_years(fs: s3fs.S3FileSystem, zarr_stores: List[str]) -> bool:
    """Check if model has all required years"""
    available_years = set()

    for store in zarr_stores:
        # Extract year from zarr store path
        year_match = re.search(r"_(\d{4})\.zarr$", store)
        if year_match:
            available_years.add(int(year_match.group(1)))

    required_years = (HISTORICAL_YEARS == available_years) or (
        FUTURE_YEARS == available_years
    )

    return required_years


def decade_month_calc(ds: xr.Dataset, time_dim: str = "time") -> xr.Dataset:
    """Calculates the climatological mean by decade and month.

    This function computes the decade-by-decade average for each month in the provided dataset.
    The process involves averaging values across each decade for each month separately.
    For instance, for the 2050s, the function calculates the average values for January, February,
    March, and so on, resulting in 12 averaged values corresponding to each month of the 2050s.
    This approach preserves seasonal variability while smoothing out interannual variability
    within each decade.

    The function performs the following steps:
    1. Assigns new coordinates to the dataset:
       - `decade`: Represents the decade (e.g., 2050 for the 2050s).
       - `month`: Represents the month (1 for January, 2 for February, etc.).
    2. Creates a combined `decade_month` coordinate, formatted as "YYYY-MM",
       where "YYYY" is the starting year of the decade, and "MM" is the month.
    3. Groups the dataset by the `decade_mon
    """
    ds = ds.assign_coords(
        decade=(ds["time.year"] // 10) * 10, month=ds["time"].dt.month
    )

    ds = ds.assign_coords(
        decade_month=(
            time_dim,
            [
                f"{decade}-{month:02d}"
                for decade, month in zip(ds["decade"].values, ds["month"].values)
            ],
        )
    )

    ds = ds.groupby("decade_month").mean()

    return ds


def reduce_model_stats(da: xr.DataArray) -> xr.Dataset:
    """
    Reduces a DataArray by computing statistical metrics (mean, median, stddev, etc.)
    across the 'model' dimension. This creates the climate ensemble mean

    Args:
        da (xr.DataArray): Input DataArray with a 'model' dimension.

    Returns:
        xr.Dataset: Dataset containing statistical metrics as variables.
    """
    # Compute metrics
    mean = da.mean(dim="model")
    median = da.median(dim="model")
    stddev = da.std(dim="model")
    min_val = da.min(dim="model")
    max_val = da.max(dim="model")
    q1 = da.quantile(0.25, dim="model").drop("quantile")
    q3 = da.quantile(0.75, dim="model").drop("quantile")
    quartiles = da.chunk({"model": -1}).quantile(
        [0.25, 0.75], dim="model"
    )
    sample_size = len(
        da.attrs.get("ensemble_members", [])
    )  # Number of climate models used when calculating stats

    # Create a new Dataset with the computed statistics
    stats_ds = xr.Dataset(
        {
            "value_mean": mean,
            "value_median": median,
            "value_stddev": stddev,
            "value_min": min_val,
            "value_max": max_val,
            "value_q1": q1,#quartiles.sel(quantile=0.25).drop("quantile"),
            "value_q3": q3,#quartiles.sel(quantile=0.75).drop("quantile"),
        },
        attrs=da.attrs,  # Copy original attributes, if any
    )
    stats_ds.attrs["sample_size"] = sample_size
    return stats_ds


def load_data(
    s3_bucket: str,
    s3_prefix: str,
    ssp: str,
    climate_variable: str,
    bbox: dict,
) -> xr.DataArray:
    """Reads all valid Zarr stores in the given S3 directory"""
    data = []

    fs = s3fs.S3FileSystem()
    pattern = f"s3://{s3_bucket}/{s3_prefix}/*"
    model_paths = fs.glob(pattern)

    # TODO: REMOVE LIST SLICE FOR FULL RUN!!!
    for model_path in model_paths[:2]:
        model_name = model_path.rstrip("/").split("/")[-1]
        logger.info(f"Validating model: {model_name}")

        # Check if model has all required SSPs
        if not validate_model_ssp(fs, model_path, ssp):
            logger.warning(f"Skipping {model_name}: missing required SSP")
            continue

        # Get all zarr stores for this model and SSP
        model_pattern = f"{model_path}/ssp{ssp}/*/{climate_variable}_day_*.zarr"
        zarr_stores = fs.glob(model_pattern)

        # Check if model has all required years
        if not validate_model_years(fs, zarr_stores):
            logger.warning(f"Skipping {model_name}: missing required years")
            continue

        # Convert to full S3 URIs
        zarr_uris = [f"s3://{path}" for path in zarr_stores]

        logger.info(f"Loading validated model: {model_name}")
        _ds = xr.open_mfdataset(
            zarr_uris,
            engine="zarr",
            combine="by_coords",
            parallel=True,
            preprocess=decade_month_calc,
        )
        _da = _ds[climate_variable]
        _da = _da.assign_coords({constants.X_DIM: (((_da[constants.X_DIM] + 180) % 360) - 180)})
        _da = _da.sortby(constants.X_DIM)

        # Bbox currently only in -180-180 lon
        # TODO: Add better error and case handling
        if bbox:
            _da = _da.sel(
                {
                    constants.Y_DIM: slice(bbox["min_lat"], bbox["max_lat"]),
                    constants.X_DIM: slice(bbox["min_lon"], bbox["max_lon"]),
                },
            )
        _da = _da.assign_coords(model=model_name)
        _da = _da.expand_dims("model")
        data.append(_da)

        logger.info(f"{model_name} loaded")

    da = xr.combine_nested(data, concat_dim=["model"])
    da = da.assign_attrs(ensemble_members=da.model.values)

    chunks = {
        "decade_month": 12,  # All in one chunk since it's usually small
        constants.X_DIM: "auto",
        constants.Y_DIM: "auto",
        "model": -1  # All models in one chunk for ensemble calculations
    }

    da = da.chunk(chunks)

    return da


def main(
    ssp: str,
    s3_bucket: str,
    s3_prefix: str,
    climate_variable: str,
    crs: str,
    bbox: dict,
) -> xr.Dataset:
    """Processes climate data

    Args:
        file_directory (str): Directory to open files from
        crs (str): Coordinate Refernce System of climate data
        bbox (dict): Dict with keys (min_lon, min_lat, max_lon, max_lat) to filter data
        time_dim (str): The name of the time dimension in the dataset
        climatology_mean_method (str): The method by which to average climate variable over time.
        derived_metadata_key (str): Keyname to store custom metadata in

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    da = load_data(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        ssp=ssp,
        climate_variable=climate_variable,
        bbox=bbox,
    )

    ds = reduce_model_stats(da)

    logger.info("Xarray dataset created")

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=constants.X_DIM, y_dim=constants.Y_DIM, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    

    return ds


if __name__ == "__main__":
    main()
