from airflow.operators.generic_transfer import GenericTransfer
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone

"""
    NOTE:

        This is a workaround for the expand_kwargs "bug". Naturally when you pass the `sql` and `preoperator` arguments to GenericTransfer
        as file paths, it knows that it is a file path and gets the content inside the file as the query, but apparently when
        you pass these arguments inside a dict using the expand_kwargs feature it does not, raising an error.  the solution involves 
        implementing tasks to fetch and read the SQL files using Python open.
"""

# This ideally should be passed as a dag decorator argument
TEMPLATE_SEARCHPATH = "/opt/airflow/include"

# tables to be transferred
TABLES = [
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film_actor",
    "film_category",
    "film",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store",
]


@dag(
    schedule="@daily",
    description="extract data from the transactional db and populate the analytical one",
    start_date=datetime(2022, 10, 9, 0, tzinfo=timezone("America/Sao_Paulo")),
    catchup=False,
)
def extract_and_load_tables():

    @task(task_id="generate_tables_transfer_configs")
    def generate_tables_configs(tables: list = TABLES) -> list:
        """
        generates a dictionary that maps each input table name to
        its corresponding SQL queries and output destination table.
        """
        tables_configs = []

        # need to assign the template_searchpath manually
        sql_folder = f"{TEMPLATE_SEARCHPATH}/sql/transactional"
        preoperator_folder = f"{TEMPLATE_SEARCHPATH}/sql/analytical"

        default_preoperator_file = "create_[TABLE]_table.sql"
        default_sql_file = "get_[TABLE].sql"

        for table in tables:
            # have to open the file and read the content on it manually
            sql_path = f"{sql_folder}/{default_sql_file.replace('[TABLE]',table)}"
            preoperator_path = f"{preoperator_folder}/{default_preoperator_file.replace('[TABLE]',table)}"
            sql_query = open(sql_path, "r").read()
            preoperator_query = open(preoperator_path, "r").read()

            # append the table config to the configs list
            data = {
                "sql": sql_query,
                "destination_table": table,
                "preoperator": preoperator_query,
            }
            tables_configs.append(data)

        return tables_configs

    # instantiates the GenericTransfer operator, sets static variables using .partial() 
    # and use .expand_kwargs() to execute the operator once for each config in the tables_configs list
    transfer_operator = GenericTransfer.partial(
        task_id="transfer_tables_to_analytical_db",
        source_conn_id="transactional",
        destination_conn_id="analytical",
    ).expand_kwargs(generate_tables_configs())

    transfer_operator


extract_and_load_tables()
