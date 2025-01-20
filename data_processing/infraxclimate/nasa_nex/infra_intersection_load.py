import io
import json
import logging
import time
import random
from typing import Dict

import pandas as pd
import psycopg2 as pg
from psycopg2 import sql

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CLIMATE_SCHEMA = "climate"

TEMP_TABLE_COLUMNS = [
    "osm_id",
    "month",
    "decade",
    "ssp",
    "value_mean",
    "value_median",
    "value_stddev",
    "value_min",
    "value_max",
    "value_q1",
    "value_q3",
    "metadata",
]

def generate_random_table_id():
    timestamp = int(time.time())  # Milliseconds since epoch
    random_part = random.randint(1000, 9999)  # Random 4-digit number
    return f"{timestamp}{random_part}"

def main(
    df: pd.DataFrame,
    ssp: int,
    climate_variable: str,
    conn: pg.extensions.connection,
    metadata: Dict,
):

    # Adds columns needed for temp table
    df["ssp"] = ssp
    data_load_table = f"nasa_nex_{climate_variable}"

    # Random ID needed if multiple laod process running at once
    random_table_id = generate_random_table_id()
    temp_table_name = f"nasa_nex_temp_{random_table_id}"

    # Reads data into memory
    sio = io.StringIO()
    sio.write(df[TEMP_TABLE_COLUMNS].to_csv(index=False, header=False))
    sio.seek(0)

    create_nasa_nex_temp_table = sql.SQL(
    """
    CREATE TEMP TABLE {temp_table} (
        osm_id BIGINT,
        month INT,
        decade INT,
        ssp int,
        value_mean FLOAT NOT NULL,
        value_median FLOAT NOT NULL,
        value_stddev FLOAT NOT NULL,
        value_min FLOAT NOT NULL,
        value_max FLOAT NOT NULL,
        value_q1 FLOAT NOT NULL,
        value_q3 FLOAT NOT NULL,
        metadata JSONB
    );
    """
    ).format(temp_table=sql.Identifier(temp_table_name))

    copy_nasa_nex_temp = sql.SQL(
    """
    COPY {temp_table}
    FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',')
    """
    ).format(temp_table=sql.Identifier(temp_table_name))

    drop_nasa_nex_temp = sql.SQL(
    """
    DROP TABLE {temp_table};
    """
    ).format(temp_table=sql.Identifier(temp_table_name))

    insert_nasa_nex = sql.SQL(
    """
    INSERT INTO {climate_schema}.{table} (osm_id, month, decade, ssp, value_mean, value_median, value_stddev, value_min, value_max, value_q1, value_q3, metadata)
            SELECT temp.osm_id, temp.month, temp.decade, temp.ssp, temp.value_mean, temp.value_median, temp.value_stddev, temp.value_min, temp.value_max, temp.value_q1, temp.value_q3, temp.metadata 
            FROM {temp_table} temp
    ON CONFLICT DO NOTHING
    """
    ).format(
        table=sql.Identifier(data_load_table),
        temp_table=sql.Identifier(temp_table_name),
        climate_schema=sql.Identifier(CLIMATE_SCHEMA),
    )

    # Executes database commands
    with conn.cursor() as cur:

        cur.execute(create_nasa_nex_temp_table)
        cur.copy_expert(copy_nasa_nex_temp, sio)
        logger.info(f"{climate_variable} Temp Table Loaded")

        cur.execute(insert_nasa_nex)
        logger.info(f"{climate_variable} Table Loaded")

        # Cleanup for next pipeline run
        cur.execute(drop_nasa_nex_temp)

    conn.commit()
