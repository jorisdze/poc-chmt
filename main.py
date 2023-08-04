import psycopg2
from config import config

import os

from google.cloud.sql.connector import Connector, IPTypes
import pg8000

import sqlalchemy
from sqlalchemy import text

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of Postgres.

    Uses the Cloud SQL Python Connector package.
    """
    # Note: Saving credentials in environment variables is convenient, but not
    # secure - consider a more secure solution such as
    # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
    # keep secrets safe.

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            "tonal-limiter-394416:us-central1:poc-chaumet",
            "pg8000",
            user="postgres",
            password="!Ven2023",
            db="chmt",
            ip_type=ip_type,
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # ...
    )
    print(pool)
    return pool



if __name__ == '__main__':

    stmt = sqlalchemy.text(
        "CREATE TABLE IF NOT EXISTS fichiers (id_fichier serial PRIMARY KEY, nom_fichier VARCHAR(100) NOT NULL,date_reception TIMESTAMP NOT NULL,date_ingestion TIMESTAMP NOT NULL);"
    )
    conn=connect_with_connector()
    try:
        with conn.connect() as connection:
            connection.execute(stmt)
            connection.commit()
            print(connection)
            print("pass!")
    except Exception as error:
        print("An error occurred:", type(error).__name__)
        print("An error occurred:", error)

