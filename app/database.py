import psycopg2 as pg
from psycopg2.extensions import connection
from psycopg2 import sql
import os
import logging

from typing import Tuple, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
                database=os.environ["PG_DBNAME"],
                user=os.environ["PG_USER"],
                password=os.environ["PG_PASSWORD"],
                host=os.environ["PG_HOST"],
            )
            break
        except Exception as e:
            logger.warning("Could not connect to database, retrying...")
            tries += 1
    if tries < 3:
        logger.error("Could not connect to database after retries.")
        raise ConnectionRefusedError()
    return conn

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

