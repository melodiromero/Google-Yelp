from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from etl_python_task import *



default_args = {
    'owner': 'jDiego',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = "ETL_piloto_v01",
    default_args = default_args,
    start_date = datetime(2023, 10, 14),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_locahost_conn',
        sql = """
            CREATE TABLE IF NOT EXISTS metadata_google_reviews(
                name_x VARCHAR,
                address TEXT,
                gmap_id TEXT,
                latitude TEXT,
                longitude TEXT,
                category TEXT,
                avg_rating NUMERIC,
                num_of_reviews INTEGER,
                hours TEXT,
                MISC TEXT,
                state TEXT,
                relative_results TEXT,
                url TEXT,
                us_state TEXT,
                user_id TEXT,
                name_y TEXT,
                time TIME,
                rating NUMERIC,
                text TEXT,
                year INTEGER
                );
                """
    )




    task2 = PythonOperator(
        task_id = 'google_maps_transform_task',
        python_callable = google_maps_and_metadata_merged_reviews,
    )

    task3 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id = "postgres_locahost_conn",
        sql = """
            TRUNCATE TABLE metadata_google_reviews;
        """
    )

    task4 = PythonOperator(
        task_id = 'load_to_postgres',
        python_callable = load_data_to_postgres
    )

    task5 = PythonOperator(
        task_id = "erase_data_in_temp_folder",
        python_callable = erase_archive
    )


    task1 >> [task2, task3] >> task4 >> task5