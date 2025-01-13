"""
Module for interfacing and connecting to a Postgres instance

"""

import psycopg2 as pg
from psycopg2.extensions import connection
from psycopg2 import sql
import os
import logging


from typing import Tuple, Any

from . import utils

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Stored in SSM for security
PG_DBNAME = utils.get_parameter(os.environ["PGDBNAME"])
PG_USER = utils.get_parameter(os.environ["PGUSER"])
PG_PASSWORD = utils.get_parameter(os.environ["PGPASSWORD"])
PG_HOST = utils.get_parameter(os.environ["PGHOST"])

def get_database_conn() -> connection:
    """Gets Postgres database connection

    Raises:
        ConnectionRefusedError: If after 3 retries conenction fails

    Returns:
        connection: return of psycopg2.connect()
    """
    tries = 0
    while tries < 3:
        try:
            conn = pg.connect(
                database=PG_DBNAME,
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
            )
            return conn
        except Exception as e:
            logger.warning(f"Could not connect to database {e}, retrying...")
            tries += 1
    if tries >= 3:
        logger.error("Could not connect to database after retries.")
        raise ConnectionRefusedError("Postgres database could not be accessed")
    

def execute_query(query: sql.SQL, params: Tuple[str] = None) -> Any:
    """Executes provided PostgreSQL query 

    Args:
        query (sql.SQL): psycopg2 sql object containing database query
        params (Tuple[str], optional): Query parameters. Defaults to None.
    """

    conn = get_database_conn()
    with conn.cursor() as cur:
        cur.execute(query, params)
        result = cur.fetchall()

    return result

