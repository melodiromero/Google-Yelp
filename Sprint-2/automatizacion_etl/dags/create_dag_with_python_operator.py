from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_arg = {
    'owner': 'jDiego',
    'retries': 5,
    'retry_dalay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello World!\n\t My name is {first_name} {last_name} and  and I am {age} old")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Jerry")
    ti.xcom_push(key="last_name", value="Fridman")

def get_age(ti):
    ti.xcom_push(key="age", value=19)

with DAG(
    default_args = default_arg,
    dag_id = "our_dag_with_python_operator_v06",
    description = "Out first dag using python operator",
    start_date = datetime(2023, 10, 8),
    schedule_interval = "@daily"
) as dag:
    task1 = PythonOperator(
        task_id = "greet",
        python_callable = greet,
        # op_kwargs = {'age': 28}
    )

    task2 = PythonOperator(
        task_id = "get_name",
        python_callable = get_name
    )

    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age
    )

    [task2, task3] >> task1 