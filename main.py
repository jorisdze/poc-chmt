import psycopg2
from config import config

import os

from google.cloud.sql.connector import Connector, IPTypes
from google.cloud import storage
import pg8000
import requests
import google.auth
import google.auth.transport.requests
import json

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

def execute_statement(stmt):
    conn = connect_with_connector()
    try:
        with conn.connect() as connection:
            connection.execute(stmt)
            connection.commit()
            print(connection)
            print("pass!")
    except Exception as error:
        print("An error occurred:", type(error).__name__)
        print("An error occurred:", error.__cause__)


def create_request(bucket,project_id,instance_id,access_token):
    url = "https://sqladmin.googleapis.com/v1/projects/{id}/instances/{instance}/import".format(id = project_id,instance=instance_id)
    var = {
        "importContext":
            {
                "fileType": "CSV",
                "uri": "gs://{bucket_name}/ingest/test.csv".format(bucket_name=bucket),
                "database": "chmt",
                "csvImportOptions":
                    {
                        "table": "test_insert"
                    }
            }
        }
    result=requests.post(url, json=var,headers={'Content-Type':'application/json',
               'Authorization': 'Bearer {}'.format(access_token)})

    dict_respons= json.loads(result.text)
    print(dict_respons["selfLink"])
    get_status = requests.get(dict_respons["selfLink"],headers={'Content-Type':'application/json',
               'Authorization': 'Bearer {}'.format(access_token)})
    print(get_status.text)




def list_bucket():
    storage_client = storage.Client(project="tonal-limiter-394416")
    BUCKET_NAME = "poc-chaumet"
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix='ingest/')

    for blob in blobs:
        print(blob.name)



if __name__ == '__main__':
    stmt_fic_table = sqlalchemy.text(
        """CREATE TABLE IF NOT EXISTS fichiers (
            id_fichier serial PRIMARY KEY,
            nom_fichier VARCHAR(100) NOT NULL,
            date_reception TIMESTAMP NOT NULL,
            date_ingestion TIMESTAMP NOT NULL);"""
    )
    stmt_client_table = sqlalchemy.text(
        """CREATE TABLE IF NOT EXISTS client_to_update (
            id serial PRIMARY KEY,
            nom VARCHAR(100) NOT NULL,
            prenom VARCHAR(100) NOT NULL,
            birthdate VARCHAR(100) NOT NULL,
            tel VARCHAR(100) NOT NULL);"""
    )

    stmt_client_table_test = sqlalchemy.text(
        """CREATE TABLE IF NOT EXISTS test_insert (
            nom VARCHAR(100) NOT NULL);"""
    )

    #
    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # getting request object
    auth_req = google.auth.transport.requests.Request()

    print(credentials.valid)  # prints False
    credentials.refresh(auth_req)  # refresh token
    # cehck for valid credentials
    print(credentials.valid)  # prints True
    print(credentials.token)  # prints token

    #execute_statement(sqlalchemy)
    #execute_statement(stmt_load_data_test)
    create_request("poc-chaumet", "tonal-limiter-394416", "poc-chaumet",credentials.token)

