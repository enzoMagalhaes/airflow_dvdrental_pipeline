from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from datetime import datetime, timedelta
from pendulum import timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

def save_data(df,filename:str):
    outdir = '/opt/airflow/dags/data'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    df.to_csv(f'{outdir}/{filename}',index=False)

def get_actor_table():
    query = """SELECT * FROM public.actor"""
    postgres_hook = PostgresHook(postgres_conn_id='transactional',schema='dvdrental')
    sales = postgres_hook.get_pandas_df(sql=query)
    save_data(sales,'actor.csv')
    
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

    get_actor_table = PythonOperator(
        task_id = 'get_actor_table',
        python_callable  = get_actor_table
    )


    start >> get_actor_table
