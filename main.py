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
import config

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
    conf = config.config()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect("tonal-limiter-394416:us-central1:poc-chaumet",
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


def create_request(bucket, project_id,
                   instance_id, access_token,
                   folder, filename,
                   database, tablename):
    url = ("https://sqladmin.googleapis.com/"
           "v1/projects/{id}/instances/"
           "{instance}/import"
           .format(id=project_id,
                   instance=instance_id))
    var = {
        "importContext":
            {
                "fileType": "CSV",
                "uri": ("gs://{bucket_name}/"
                        "{folder}/"
                        "{filename}.csv"
                        .format(bucket_name=bucket,
                                folder=folder,
                                filename=filename)),
                "database": database,
                "csvImportOptions":
                    {
                        "table": tablename
                    }
            }
    }
    result = requests.post(url, json=var, headers={'Content-Type': 'application/json',
                                                   'Authorization': 'Bearer {}'.format(access_token)})

    def get_req_status():
        try:
            return requests.get(dict_respons["selfLink"], headers={'Content-Type': 'application/json',
                                                                   'Authorization': 'Bearer {}'.format(access_token)})
        except Exception as error:
            print("An error has occurred when get req status. error : {}".format(error.__cause__))

    dict_respons = json.loads(result.text)
    print(dict_respons["selfLink"])

    dict_status = json.loads(get_req_status().text)
    print(dict_status["status"])

    status = dict_status["status"]
    while status == "PENDING" or status == "RUNNING":
        current_status = json.loads(get_req_status().text)
        status = current_status["status"]
        print(status)

    print(json.loads(get_req_status().text))


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

    stmt_client_test = sqlalchemy.text(
        """CREATE TABLE IF NOT EXISTS client_to_update_test (
            nom VARCHAR(100) NOT NULL,
            prenom VARCHAR(100) NOT NULL,
            birthdate VARCHAR(100) NOT NULL,
            tel VARCHAR(100) NOT NULL);"""
    )

    stmt_client_test_2 = sqlalchemy.text(
        """SELECT 1;"""
    )

    #
    # getting the credentials and project details for gcp project
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    # getting request object
    auth_req = google.auth.transport.requests.Request()
    print(credentials.valid)  # prints False
    credentials.refresh(auth_req)  # refresh token
    # check for valid credentials
    print(credentials.valid)  # prints True
    print(credentials.token)  # prints token
    conf = config.config()
    print(conf["user"])
    print(conf["database"])
    print(conf["password"])
    print(conf["name"])

    execute_statement(stmt_client_test_2)

    # execute_statement(sqlalchemy)
    # execute_statement(stmt_load_data_test)
    # create_request("poc-chaumet",
    #                "tonal-limiter-394416",
    #                "poc-chaumet",
    #                 credentials.token,
    #                "ingest",
    #                "sample",
    #                "chmt",
    #                "client_to_update_test")
