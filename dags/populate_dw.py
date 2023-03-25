from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from datetime import datetime, timedelta
from pendulum import timezone

from functions.helpers import get_table_data

with DAG(
    dag_id='populate_dw',
    default_args={
        'owner': 'etl_owner',
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    },
    description='extract data from the transactional db and populate the DW',
    start_date=datetime(2022, 10, 9, 0, tzinfo=timezone("America/Sao_Paulo")),
    schedule_interval='0 0 * * *',
    template_searchpath='/opt/airflow/include',
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    get_actor_data_to_analytical_db = GenericTransfer(
        task_id='get_actor_data_to_analytical_db',
        sql = 'sql/transactional/get_actor.sql',
        destination_table = 'actor',
        source_conn_id = 'transactional',
        destination_conn_id = 'analytical',
        preoperator = 'sql/analytical/create_or_truncate_actor.sql'
    )


    start >> get_actor_data_to_analytical_db
