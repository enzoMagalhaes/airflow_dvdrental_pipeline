# FOR TESTING PURPOSES ONLY! DO NOT SET CONNECTIONS LIKE THIS IN A REAL-WORLD PROJECT!
from airflow import settings
from airflow.models import Connection

transactional_db_conn = Connection(
    conn_id="transactional",
    conn_type="postgres",
    host="172.17.0.1",
    login="postgres",
    password="password",
    port=5432,
    schema="dvdrental",
)

analytical_db_conn = Connection(
    conn_id="analytical",
    conn_type="postgres",
    host="172.17.0.1",
    login="postgres",
    password="password",
    port=5440,
    schema="analytics",
)

session = settings.Session()  # get the session
session.add_all([transactional_db_conn, analytical_db_conn])
session.commit()  # it will insert the connection object programmatically.
