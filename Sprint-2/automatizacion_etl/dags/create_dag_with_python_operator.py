from datetime import timedelta, datetime

# from etl_python_task import *

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_versions():
    import pandas
    import sklearn
    import numpy
    import dask
    import pyarrow

    print(pandas.__version__)
    print(sklearn.__version__)
    print(numpy.__version__)
    print(dask.__version__)
    print(pyarrow.__version__)


default_arg = {
    'owner': 'jDiego',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args = default_arg,
    dag_id = "our_dag_with_python_operator_v01",
    description = "Out first dag using python operator",
    start_date = datetime(2023, 10, 12),
    schedule_interval = "@daily",
    catchup = True
) as dag:
    task1 = PythonOperator(
        task_id = "python_package_versions",
        python_callable = print_versions
    )

    # task2 = PythonOperator(
    #     task_id = "test_imports_from_py",
    #     python_callable = hello_word
    # )

    task1 # >> task2