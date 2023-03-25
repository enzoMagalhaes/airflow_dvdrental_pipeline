# FOR TESTING PURPOSES ONLY! DO NOT SET CONNECTIONS LIKE THIS IN PRODUCTION!
from airflow import settings
from airflow.models import Connection

transactional_db_conn = Connection(
        conn_id='transactional',
        conn_type='postgres',
        host='172.17.0.1',
        login='postgres',
        password='password',
        port=5432,
        schema='dvdrental'
)

analytical_dw_conn = Connection(
        conn_id='analytical',
        conn_type='postgres',
        host='172.17.0.1',
        login='postgres',
        password='password',
        port=5440,
        schema='analytical'
)

session = settings.Session() # get the session
session.add_all([
    transactional_db_conn,
    analytical_dw_conn
])
session.commit() # it will insert the connection object programmatically.
