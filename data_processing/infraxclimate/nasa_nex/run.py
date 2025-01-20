import logging
import os

import pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    ssps = os.getenv("SSP")

    # Split the string into a list and converts to ints
    try:
        ssp_list = list(map(int, ssps.split(",")))
    except Exception as e:
        logger.error(f"Failed to parse SSP ENV var list: {str(e)}")

    for ssp in ssp_list:
        try:
            logger.info(f"STARTING PIPELINE FOR SSP {str(ssp)}")
            pipeline.main(ssp=ssp)
            logger.info(f"PIPELINE SUCCEEDED FOR SSP {str(ssp)}")
        except Exception as e:
            logger.error(f"WARNING: PIPELINE FAILED FOR SSP {str(ssp)}: {str(e)}")
