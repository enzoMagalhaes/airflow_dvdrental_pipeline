import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag()

@pytest.fixture(scope="session")
def dag():
    return DagBag().get_dag('extract_and_load_tables')
