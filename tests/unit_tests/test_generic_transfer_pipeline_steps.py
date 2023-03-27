import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import TaskInstance
from datetime import datetime
from psycopg2.errors import UndefinedTable


class TestMultipleTableTransferPipeline:
    input_db_hook = PostgresHook(postgres_conn_id="transactional", database="dvdrental")
    output_db_hook = PostgresHook(postgres_conn_id="analytical", database="analytics")

    @pytest.mark.dependency
    def test_transfer_configs_generation(self, dag):
        """
        Tests if config generation task works properly
        """

        # all the keys that the config dicts must have
        TRANSFER_CONFIG_ARGS = ["sql", "destination_table", "preoperator"]

        # runs the task python function and get the output
        configs_operator = dag.get_task("generate_tables_transfer_configs")
        transfer_configs = configs_operator.python_callable()

        # assert that the output is a list of dicts
        assert isinstance(transfer_configs, list)
        sample_config = transfer_configs[0]
        assert isinstance(sample_config, dict)

        # assert that dicts have all the specified args
        for key in sample_config.keys():
            assert key in TRANSFER_CONFIG_ARGS

    @pytest.mark.dependency(depends=["test_transfer_configs_generation"])
    def test_GenericTransfer_inputs_data_as_expected(self, dag):
        """
        Checks if data is correctly transfered from input do output db
        """

        # load transfer configs
        configs_operator = dag.get_task("generate_tables_transfer_configs")
        transfer_configs = configs_operator.python_callable()

        # get load table task
        load_table_task = dag.get_task("load_tables_task")

        # checks data transfer for each table
        for config in transfer_configs:
            table = config["destination_table"]

            # drop table if exists from output db and assert table was dropped
            self.output_db_hook.run(sql=f"DROP TABLE IF EXISTS {table} CASCADE;")
            with pytest.raises(UndefinedTable) as assert_table_was_dropped:
                self.output_db_hook.get_records(sql=f"SELECT 1 FROM {table} LIMIT 1")

            # executes transfer operation
            transfer_operator = load_table_task.operator_class
            partial_args = load_table_task.partial_kwargs
            transfer_operator(
                task_id="test_generic_transfer_instance",
                sql=config["sql"],
                destination_table=table,
                source_conn_id=partial_args["source_conn_id"],
                destination_conn_id=partial_args["destination_conn_id"],
                preoperator=config["preoperator"],
            ).execute(dict())

            # assert that table was created and the it has the same number of lines as the input db table
            count_query = f"SELECT COUNT(*) FROM {table};"
            input_lines_count = self.input_db_hook.get_records(sql=count_query)[0][0]
            assert (
                self.output_db_hook.get_records(sql=count_query)[0][0]
                == input_lines_count
            )

            # assert that all the data types match the expected types (same ones as the input db)
            sample_data_query = f"SELECT * FROM {table} LIMIT 5"
            input_data_types = self.input_db_hook.get_pandas_df(
                sql=sample_data_query
            ).dtypes
            output_data_types = self.output_db_hook.get_pandas_df(
                sql=sample_data_query
            ).dtypes
            for i in range(len(input_data_types)):
                assert input_data_types[i] == output_data_types[i]
