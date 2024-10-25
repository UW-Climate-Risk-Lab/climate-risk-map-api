import psycopg2
import os

def get_database():
    conn = psycopg2.connect() # TODO: Add appropriate environment details
    return conn