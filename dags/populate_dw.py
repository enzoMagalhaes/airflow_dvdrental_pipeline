from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from datetime import datetime, timedelta
from pendulum import timezone

from functions.generic_transfer_wrapper import (
    generic_transfer_operator_wrapper as generic_transfer,
)

with DAG(
    dag_id="populate_analytical_db",
    default_args={
        "owner": "etl_owner",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    description="extract data from the transactional db and populate the analytical database",
    start_date=datetime(2022, 10, 9, 0, tzinfo=timezone("America/Sao_Paulo")),
    schedule_interval="0 0 * * *",
    template_searchpath="/opt/airflow/include",
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="load_tables_to_analytical_db") as load_tables_to_analytical_db:
        load_actor = generic_transfer(
            dag=dag,
            task_id="get_actor_data_to_analytical_db",
            sql="get_actor.sql",
            destination_table="actor",
            preoperator="create_actor_table.sql",
        )
        load_film_actor = generic_transfer(
            dag=dag,
            task_id="get_film_actor_data_to_analytical_db",
            sql="get_film_actor.sql",
            destination_table="film_actor",
            preoperator="create_film_actor_table.sql",
        )
        load_address = generic_transfer(
            dag=dag,
            task_id="get_address_data_to_analytical_db",
            sql="get_address.sql",
            destination_table="address",
            preoperator="create_address_table.sql",
        )
        load_category = generic_transfer(
            dag=dag,
            task_id="get_category_data_to_analytical_db",
            sql="get_category.sql",
            destination_table="category",
            preoperator="create_category_table.sql",
        )
        load_city = generic_transfer(
            dag=dag,
            task_id="get_city_data_to_analytical_db",
            sql="get_city.sql",
            destination_table="city",
            preoperator="create_city_table.sql",
        )
        load_country = generic_transfer(
            dag=dag,
            task_id="get_country_data_to_analytical_db",
            sql="get_country.sql",
            destination_table="country",
            preoperator="create_country_table.sql",
        )
        load_customer = generic_transfer(
            dag=dag,
            task_id="get_customer_data_to_analytical_db",
            sql="get_customer.sql",
            destination_table="customer",
            preoperator="create_customer_table.sql",
        )
        load_film_category = generic_transfer(
            dag=dag,
            task_id="get_film_category_data_to_analytical_db",
            sql="get_film_category.sql",
            destination_table="film_category",
            preoperator="create_film_category_table.sql",
        )
        load_film = generic_transfer(
            dag=dag,
            task_id="get_film_data_to_analytical_db",
            sql="get_film.sql",
            destination_table="film",
            preoperator="create_film_table.sql",
        )
        load_inventory = generic_transfer(
            dag=dag,
            task_id="get_inventory_data_to_analytical_db",
            sql="get_inventory.sql",
            destination_table="inventory",
            preoperator="create_inventory_table.sql",
        )
        load_language = generic_transfer(
            dag=dag,
            task_id="get_language_data_to_analytical_db",
            sql="get_language.sql",
            destination_table="language",
            preoperator="create_language_table.sql",
        )
        load_payment = generic_transfer(
            dag=dag,
            task_id="get_payment_data_to_analytical_db",
            sql="get_payment.sql",
            destination_table="payment",
            preoperator="create_payment_table.sql",
        )
        load_rental = generic_transfer(
            dag=dag,
            task_id="get_rental_data_to_analytical_db",
            sql="get_rental.sql",
            destination_table="rental",
            preoperator="create_rental_table.sql",
        )
        load_staff = generic_transfer(
            dag=dag,
            task_id="get_staff_data_to_analytical_db",
            sql="get_staff.sql",
            destination_table="staff",
            preoperator="create_staff_table.sql",
        )
        load_store = generic_transfer(
            dag=dag,
            task_id="get_store_data_to_analytical_db",
            sql="get_store.sql",
            destination_table="store",
            preoperator="create_store_table.sql",
        )

    start >> load_tables_to_analytical_db
