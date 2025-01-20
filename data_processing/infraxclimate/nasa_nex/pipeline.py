import argparse
import logging
import os
import tempfile

from psycopg2 import pool

import generate_geotiff
import infra_intersection
import infra_intersection_load
import process_climate
import utils
import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


LOAD_GEOTIFFS = False # For debugging, loading geotiffs takes time and 

PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]


def setup_args():
    parser = argparse.ArgumentParser(description="Process climate data for a given SSP")

    parser.add_argument("--ssp", required=True, help="SSP Scenario")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument("--s3-prefix", required=True, help="S3 base prefix for climate data")
    parser.add_argument("--s3-prefix-geotiff", required=True, help="S3 base prefix for outputting geotiffs")
    parser.add_argument(
        "--climate-variable", required=True, help="Climate variable to process"
    )
    parser.add_argument("--crs", required=True, help="Coordinate reference system")
    parser.add_argument(
        "--zonal-agg-method", required=True, help="Zonal aggregation method"
    )
    parser.add_argument("--state-bbox", help="State bounding box")
    parser.add_argument("--osm-category", required=True, help="OSM category")
    parser.add_argument("--osm-type", required=True, help="OSM type")
    return parser.parse_args()


def main(
    ssp: int,
    s3_bucket: str,
    s3_prefix: str,
    s3_prefix_geotiff: str,
    climate_variable: str,
    crs: str,
    zonal_agg_method: str,
    state_bbox: str,
    osm_category: str,
    osm_type: str,
):
    """Runs a processing pipeline for a given zarr store"""

    # Create connection pool with passed parameters
    connection_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=3,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

    bbox = utils.get_state_bbox(state_bbox)

    ds = process_climate.main(
        ssp=ssp,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        climate_variable=climate_variable,
        crs=crs,
        bbox=bbox
    )

    logger.info("Climate Data Processed")

    metadata = utils.create_metadata(
        ds=ds
    )

    metadata[constants.METADATA_KEY]["zonal_agg_method"] = zonal_agg_method

    with tempfile.TemporaryDirectory() as geotiff_tmpdir:
        if LOAD_GEOTIFFS:
            generate_geotiff.main(
                ds=ds,
                output_dir=geotiff_tmpdir,
                climate_variable=climate_variable,
                state=state_bbox,
                metadata=metadata,
            )
            logger.info("Geotiffs created")

            utils.upload_files(
                s3_bucket=s3_bucket,
                s3_prefix=utils.create_s3_prefix(
                    s3_prefix_geotiff,
                    climate_variable,
                    ssp,
                    "cogs",
                ),
                dir=geotiff_tmpdir,
            )
            logger.info("Geotiffs uploaded")

        infra_intersection_conn = connection_pool.getconn()
        df = infra_intersection.main(
            climate_ds=ds,
            osm_category=osm_category,
            osm_type=osm_type,
            crs=crs,
            zonal_agg_method=zonal_agg_method,
            conn=infra_intersection_conn,
            metadata=metadata  # Add metadata parameter
        )
        connection_pool.putconn(infra_intersection_conn)
        logger.info("Infrastructure Intersection Complete")

        infra_intersection_load_conn = connection_pool.getconn()
        infra_intersection_load.main(
            df=df,
            ssp=int(ssp),
            climate_variable=climate_variable,
            conn=infra_intersection_load_conn,
            metadata=metadata,
        )
        connection_pool.putconn(infra_intersection_load_conn)


if __name__ == "__main__":
    args = setup_args()
    main(
        ssp=args.ssp,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        s3_prefix_geotiff=args.s3_prefix_geotiff,
        climate_variable=args.climate_variable,
        crs=args.crs,
        zonal_agg_method=args.zonal_agg_method,
        state_bbox=args.state_bbox,
        osm_category=args.osm_category,
        osm_type=args.osm_type,
    )
