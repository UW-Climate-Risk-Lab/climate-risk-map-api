import argparse
import logging
import os

import pipeline
import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def setup_args():
    parser = argparse.ArgumentParser(description="Process climate data for a given SSP")

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


if __name__ == "__main__":
    args = setup_args()
    
    for ssp in [str(ssp) for ssp in constants.SSPS]:
        logger.info(f"STARTING PIPELINE FOR SSP {ssp}")
        pipeline.main(
            ssp=ssp,
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
        logger.info(f"PIPELINE SUCCEEDED FOR SSP {ssp}")
