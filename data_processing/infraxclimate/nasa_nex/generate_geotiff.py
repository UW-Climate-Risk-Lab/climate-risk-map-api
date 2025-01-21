import json
from pathlib import Path
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

import rioxarray
import xarray as xr

import constants

logger = logging.getLogger(__name__)

def save_geotiff(data: tuple) -> None:
    """Helper function to save individual geotiff"""
    da, output_path = data
    da.rio.to_raster(str(output_path), driver="COG")
    logger.info(f"Saved {output_path}")

def main(
    ds: xr.Dataset,
    output_dir: str,
    state: str,
    metadata: Dict,
    max_workers: int = 8,
) -> None:
    if not state:
        state = "global"

    # Prepare all the data tuples for parallel processing
    save_tasks = []
    for decade_month in ds["decade_month"].data:
        # For visualizing climate grid, we just use the mean
        _da = ds[f"value_mean"].sel(decade_month=decade_month)
        file_name = f"{decade_month}-{state}.tif"
        output_path = Path(output_dir) / file_name
        save_tasks.append((_da, output_path))

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(save_geotiff, task) for task in save_tasks]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error saving geotiff: {str(e)}")

    # Save metadata file
    metadata[constants.METADATA_KEY]["max_climate_variable_value"] = float(
        ds[f"value_mean"].max()
    )
    metadata[constants.METADATA_KEY]["min_climate_variable_value"] = float(
        ds[f"value_mean"].min()
    )

    metadata_file = f"metadata-{state}.json"
    metadata_output_path = Path(output_dir) / metadata_file
    with open(str(metadata_output_path), "w") as f:
        json.dump(metadata, f, indent=4)

    logger.info("All geotiffs and metadata saved successfully")
