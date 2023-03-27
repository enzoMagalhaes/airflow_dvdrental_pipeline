from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook


def check_table_availability(
    *,
    table_name: str = None,
    conn_id: str = "transactional",
    db_name: str = "dvdrental",
):
    """
    Checks if db table returns data with no errors.
    """

    db_hook = PostgresHook(postgres_conn_id=conn_id, schema=db_name)
    query = f"SELECT 1 FROM public.{table_name} limit 1;"
    try:
        db_hook.get_records(sql=query)
    except:
        raise Exception(
            'CONEXAO "{conn_id}" COM O BANCO DE DADOS "{db_name}", TABELA "{table_name}" MAL SUCEDIDA'
        )
