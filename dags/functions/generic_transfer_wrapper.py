from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer


def generic_transfer_operator_wrapper(
    dag: DAG,
    task_id: str,
    sql: str,
    destination_table: str,
    preoperator: str,
    source_conn_id: str = "transactional",
    destination_conn_id: str = "analytical",
    sql_folder: str = "sql/transactional",
    preoperator_folder: str = "sql/analytical",
) -> GenericTransfer:
    
    transfer_operator = GenericTransfer(
        dag=dag,
        task_id=task_id,
        sql=f"{sql_folder}/{sql}",
        destination_table=destination_table,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        preoperator=f"{preoperator_folder}/{preoperator}",
    )

    return transfer_operator
