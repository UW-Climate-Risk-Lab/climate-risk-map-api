import logging
import os
from pathlib import Path

import rioxarray
import xarray as xr

import scenariomip.utils as utils

from typing import List

logger = logging.getLogger(__name__)

TIME_AGG_METHODS = ["decade_month"]


def decade_month_calc(ds: xr.Dataset, time_dim: str) -> xr.Dataset:
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


def climate_calc(ds: xr.Dataset, time_dim: str, time_agg_method: str) -> xr.Dataset:
    """Runs climate calculations on xarray dataset. Modifys dataset in place"""
    if time_agg_method not in TIME_AGG_METHODS:
        raise ValueError(f"{time_agg_method} time aggregation method not implemented")

    if time_agg_method == "decade_month":
        ds = decade_month_calc(ds=ds, time_dim=time_dim)

    return ds

def read_data(file_directory: str, xarray_engine: str) -> xr.Dataset:
    data = []
    for file in os.listdir(file_directory):
        path = Path(file_directory) / file
        _ds = xr.open_dataset(
            filename_or_obj=str(path),
            engine=xarray_engine,
            decode_times=True,
            use_cftime=True,
            decode_coords=True,
            mask_and_scale=True,
        )
        data.append(_ds)
    
    # Dropping conflicts because the creation_date between
    # datasets was slightly different (a few mintues apart).
    # All other attributes should be the same.
    # TODO: Better handle conflicting attribute values
    ds = xr.merge(data, combine_attrs="drop_conflicts")
    return ds

def main(
    file_directory: str,
    xarray_engine: str,
    climate_variable: str,
    crs: str,
    x_dim: str,
    y_dim: str,
    convert_360_lon: bool,
    bbox: dict,
    time_dim: str,
    climatology_mean_method: str,
    derived_metadata_key: str,
) -> xr.Dataset:
    """Processes climate data

    Args:
        file_directory (str): Directory to open files from
        xarray_engine (str): Engine for Xarray to open files
        crs (str): Coordinate Refernce System of climate data
        x_dim (str): The X coordinate dimension name (typically lon or longitude)
        y_dim (str): The Y coordinate dimension name (typically lat or latitude)
        convert_360_lon (bool): If True, converts 0-360 lon values to -180-180
        bbox (dict): Dict with keys (min_lon, min_lat, max_lon, max_lat) to filter data
        time_dim (str): The name of the time dimension in the dataset
        climatology_mean_method (str): The method by which to average climate variable over time.
        derived_metadata_key (str): Keyname to store custom metadata in

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    # For the initial dataset (burntFractionAll CESM2), each SSP
    # contained 2 files, with 2 chunks of years. These can be simply merged
    ds = read_data(file_directory=file_directory, xarray_engine=xarray_engine)

    logger.info("Xarray dataset created")

    if convert_360_lon:
        ds = ds.assign_coords({x_dim: (((ds[x_dim] + 180) % 360) - 180)})
        ds = ds.sortby(x_dim)

        # Bbox currently only in -180-180 lon
        # TODO: Add better error and case handling
        if bbox:
            ds = ds.sel(
                {
                    y_dim: slice(bbox["min_lat"], bbox["max_lat"]),
                    x_dim: slice(bbox["min_lon"], bbox["max_lon"]),
                },
            )

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    ds = climate_calc(ds=ds, time_dim=time_dim, time_agg_method=climatology_mean_method)

    metadata = utils.create_metadata(
        ds=ds,
        derived_metadata_key=derived_metadata_key,
        climate_variable=climate_variable,
    )

    metadata[derived_metadata_key]["max_climate_variable_value"] = float(
        ds[climate_variable].max()
    )
    metadata[derived_metadata_key]["min_climate_variable_value"] = float(
        ds[climate_variable].min()
    )

    return ds, metadata


if __name__ == "__main__":
    main()
