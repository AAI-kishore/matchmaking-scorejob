import logging
from dotenv import load_dotenv, find_dotenv
import os
from sqlalchemy import create_engine
from contextlib import contextmanager
import enum
import urllib.parse
import logging
load_dotenv(find_dotenv())

class ConnectionParams(enum.Enum):
    username = os.getenv("DATABASE_USER")
    password = os.getenv("DATABASE_PASSWORD")
    server = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT")
    database = os.getenv("DATABASE")
    database_type = os.getenv("DATABASE_TYPE")


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



    



        
    
    

    
