import logging
from dotenv import load_dotenv, find_dotenv
import os
from sqlalchemy import create_engine
from contextlib import contextmanager
import enum
import urllib.parse
import logging
import pydata_google_auth
from google.cloud import bigquery
load_dotenv(find_dotenv())

class ConnectionParams(enum.Enum):
    username = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD")
    server = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT")
    database = os.getenv("DATABASE")
    database_type = os.getenv("DATABASE_TYPE")
    project_id= os.getenv("PROJECT_ID")


@contextmanager
def get_connection():

    if ConnectionParams.database_type.value == 'mysql':
        db_url= f"mysql://{ConnectionParams.username.value}:{urllib.parse.quote_plus(ConnectionParams.password.value)}" \
                f"@{ConnectionParams.server.value}/{ConnectionParams.database.value}"
                
        logging.debug(f"database url: {db_url}")
        engine = create_engine(db_url)
        conn= engine.connect()
        logging.debug(('connection made to database'))
    else:
        logging.error('Database type not supported')
    try:
        yield conn
        logging.debug('using the connection')
    finally:
        logging.debug('closing connection for database')
        conn.close()

def gbq_client():
    credentials = pydata_google_auth.load_user_credentials(
                        os.path.join('config','credentials.json')
                    )
    
    #getting project_id
    project_id = ConnectionParams.project_id.value
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project_id,credentials=credentials)
    return client




    



        
    
    

    