from airflow.providers.postgres.hooks.postgres import PostgresHook
# from pandas import DataFrame
import os


def save_data(df,filename:str):
    outdir = '/opt/airflow/dags/data'
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    df.to_csv(f'{outdir}/{filename}',index=False)

def get_table_data(*,table_name: str = None, conn_id: str = 'transactional', db_name: str = 'dvdrental'):
    query = f"""SELECT * FROM public.{table_name}"""
    postgres_hook = PostgresHook(postgres_conn_id=conn_id,schema=db_name)
    table_data = postgres_hook.get_pandas_df(sql=query)
    save_data(table_data,f'{table_name}.csv')
