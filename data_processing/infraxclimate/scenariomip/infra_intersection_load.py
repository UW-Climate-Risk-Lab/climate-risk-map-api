import io
import json
import logging
from typing import Dict

import pandas as pd
import psycopg2 as pg
from psycopg2 import sql

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


SCENARIOMIP_VARIABLE_TABLE = "scenariomip_variables"
SCENARIOMIP_TABLE = "scenariomip"
CLIMATE_SCHEMA = "climate"

INSERT_SCENARIOMIP_VARIABLES = sql.SQL(
    """
INSERT INTO {climate_schema}.{table} (variable, ssp, metadata) 
VALUES(%s, %s, %s) 
ON CONFLICT DO NOTHING
"""
).format(
    climate_schema=sql.Identifier(CLIMATE_SCHEMA),
    table=sql.Identifier(SCENARIOMIP_VARIABLE_TABLE),
)

CREATE_SCENARIOMIP_TEMP_TABLE = sql.SQL(
    """
CREATE TEMP TABLE scenariomip_temp (
    osm_id BIGINT,
    month INT,
    decade INT,
    variable_name TEXT,
    ssp int,
    value FLOAT
);
"""
)

TEMP_TABLE_COLUMNS = ["osm_id", "month", "decade", "variable_name", "ssp", "value"]

COPY_SCENARIOMIP_TEMP = sql.SQL(
    """
COPY scenariomip_temp
FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',')
"""
)

DROP_SCENARIOMIP_TEMP = sql.SQL(
    """
    DROP TABLE scenariomip_temp;
    """
)

INSERT_SCENARIOMIP = sql.SQL(
    """
INSERT INTO {climate_schema}.{scenariomip} (osm_id, month, decade, variable_id, value)
        SELECT temp.osm_id, temp.month, temp.decade, v.id, temp.value
        FROM scenariomip_temp temp
        INNER JOIN {climate_schema}.{scenariomip_variables} v 
        ON temp.variable_name = v.variable AND temp.ssp = v.ssp
ON CONFLICT DO NOTHING
"""
).format(
    scenariomip_variables=sql.Identifier(SCENARIOMIP_VARIABLE_TABLE),
    scenariomip=sql.Identifier(SCENARIOMIP_TABLE),
    climate_schema=sql.Identifier(CLIMATE_SCHEMA),
)


def main(
    df_scenariomip: pd.DataFrame,
    ssp: int,
    climate_variable: str,
    conn: pg.extensions.connection,
    metadata: Dict,
):

    # Adds columns needed for temp table
    df_scenariomip["ssp"] = ssp
    df_scenariomip["variable_name"] = climate_variable

    # Reads data into memory
    sio = io.StringIO()
    sio.write(df_scenariomip[TEMP_TABLE_COLUMNS].to_csv(index=False, header=False))
    sio.seek(0)

    # Executes database commands
    with conn.cursor() as cur:
        cur.execute(
            INSERT_SCENARIOMIP_VARIABLES, (climate_variable, ssp, json.dumps(metadata))
        )
        logger.info("ScenariMIP Variables and metadata inserted")

        cur.execute(CREATE_SCENARIOMIP_TEMP_TABLE)
        cur.copy_expert(COPY_SCENARIOMIP_TEMP, sio)
        logger.info("ScenarioMIP Temp Table Loaded")

        cur.execute(INSERT_SCENARIOMIP)
        logger.info("ScenarioMIP Table Loaded")

        # Cleanup for next pipeline run
        cur.execute(DROP_SCENARIOMIP_TEMP)
    conn.commit()
