# infraXclimate Engine NASA NEX Processing Pipeline

This code provides a pipeline for processing climate data, generating GeoTIFF files & uploading them to an S3 bucket, zonally aggregating OSM features and loading the results into PostGIS. The pipeline consists of several scripts that handle different stages of the processing.

A single code run will process a given climate variable for all user-defined Shared Socioeconomic Pathways (SSP), and intersect the results with a given OSM feature type.

In the future as the lab scales, this single process can be broken up into separate components.


## Table of Contents

- [Scripts](#scripts)
- [Environment Variables](#environment-variables)
- [Build](#build)

## Scripts

### `run.py`

Entry point of the process, iterates through available SSPs.

---

### `pipeline.py`

This script orchestrates the entire processing pipeline. Each run produces results for a given SSP.

---

### `process_climate.py`

This script processes the climate data. It reads NetCDF or Zarr files, processes the data using Xarray, and returns an Xarray dataset. Currently, as simple climateological mean method is used to process the climate data. Additional climate datasets my require additional methods to be added.

---

### `generate_geotiff.py`

This script generates GeoTIFF files from the processed climate data. It uses the rioxarray library to convert Xarray datasets to Cloud Optimized GeoTIFF (COG) format. These GeoTIFFs can be served to a frontend mapping application to visualize output.

---

### `infra_intersection.py`

This script queries the database (assumes PG OSM Flex Schema) and joins the features with the processed climate data using xarray and the xvec package. This will return a tabular dataframe containing each feature at different timesteps with it's associated climate value and identifier.

---

### `infra_intersection_load.py`

This script takes the output of infra_intersection.py and loads into the PostGIS database using a series fo SQL scripts. This currently utilizes the schema defined in `climate-risk-map/backend/physical-asset/database/pgosm_flex_washington/migrations`.

## Environment Variables
Environment variables can be set using by creating a .env file in this directory or in deployment. Follow sample.env for default settings.

The following environment variables are required to run the pipeline:

- `S3_BUCKET`: The name of the S3 bucket where the climate data is stored.
- `S3_BASE_PREFIX`: The base prefix for S3 paths where the climate data is stored. 

The process will pull raw data from (up to the user to provide the initial data in the specified location for each climate variable and ssp): `$S3_BUCKET://$S3_BASE_PREFIX/$CLIMATE_VARIABLE/ssp$SSP/data`


The process will output COGs into:
`$S3_BUCKET://$S3_BASE_PREFIX/$CLIMATE_VARIABLE/ssp$SSP/cogs`



- `CLIMATE_VARIABLE`: The climate variable to process.
- `SSP`: The Shared Socioeconomic Pathway (SSP) scenarios available, should be a comma delimited list.
- `XARRAY_ENGINE`: The engine to use for reading NetCDF files with Xarray.
- `CRS`: The Coordinate Reference System (CRS) for the data.
- `X_DIM`: The name of the X coordinate dimension (e.g., lon or longitude).
- `Y_DIM`: The name of the Y coordinate dimension (e.g., lat or latitude).
- `TIME_DIM`: The name of the time coordinate dimension (e.g., time)
- `CLIMATOLOGY_MEAN_METHOD`: Method to average climate variable over time. Currently, the code recoginzes "decade_month", which averages overs each decade, grouped by month. 
- `ZONAL_AGG_METHOD`: Method when zonally aggregating climate variable values to vector geometry. Common are 'mean' or 'max'
- `CONVERT_360_LON`: Whether to convert longitude values from 0-360 to -180-180.
- `STATE_BBOX`: (Optional) The bounding box for a specific state.
- `PG_DBNAME`: Name of database created by PgOSM Flex (see directory */climate-risk-map/backend/physical_asset/etl/pgosm_flex*)
- `PG_USER`: Postgres user with read access of the osm schema, and read-write access to climate schema
- `PG_HOST`: Postgres host
- `PG_PASSWORD`: PG_USER password
- `OSM_CATEGORY`: OpenStreetMap feature category to query for intersection
- `OSM_TYPE`: OpenStreetMap feature type to query for intersection
- `METADATA_KEY`: Key for additional climate metadata derived in the process

## Build

A Dockerfile is provided to containerize the application. We build with platform linux/amd64 due to geospatial library dependancies. This assumes you have raw data stored in S3 and that AWS credentials are stored in the default location.

### Dependencies
Dependencies are managed through Poetry, and a lock file is provided. The container will automatically run the process using a virtual environment created by poetry when the image was built. 

Building the image relies on a number of geospatial packages to install the python libraries found in `pyproject.toml`.

- [GDAL](https://gdal.org/en/latest/)
- [GEOS](https://libgeos.org/)


 **To build and run the Docker container:**

1. Build the Docker image:
```bash
docker build --platform linux/amd64 -t data_processing/infraxclimate/nasa_nex .
```

2. Run the Docker container:
```bash
docker run --env-file .env -v ~/.aws/credentials:/root/.aws/credentials:ro data_processing/infraxclimate/nasa_nex
```


