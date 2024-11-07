import json
from pathlib import Path
from typing import Dict

import rioxarray
import xarray as xr


def main(
    ds: xr.Dataset,
    output_dir: str,
    climate_variable: str,
    state: str,
    climatology_mean_method: str,
    metadata: Dict,
) -> None:
    # If a US state isnt specified, assume global
    if not state:
        state = "global"

    # TODO: Clean this up
    if climatology_mean_method == "decade_month":
        for decade_month in ds["decade_month"].data:
            _da = ds.sel(decade_month=decade_month)[climate_variable]
            file_name = f"{decade_month}-{state}.tif"
            output_path = Path(output_dir) / file_name
            _da.rio.to_raster(str(output_path), driver="COG")

    # Output metadata file with GeoTIFFS
    metadata_file = f"metadata-{state}.json"
    metadata_output_path = Path(output_dir) / metadata_file

    with open(str(metadata_output_path), "w") as f:
        json.dump(metadata, f, indent=4)
