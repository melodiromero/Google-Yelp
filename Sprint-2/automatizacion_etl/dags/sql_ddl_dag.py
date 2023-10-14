from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import psycopg2
from psycopg2 import sql
import pandas as pd

file_path = F"/opt/airflow/raw_data/csv_prueba.csv"

# def read_transform_save():
#     file_path = "C:\\Users\\jdieg\\Desktop\\henry\\proyectos\\Google-Yelp\\.data\\csv_prueba.csv"
#     data = pd.read_csv(file_path)
#     return data

def load_data_to_postgres():

    dbname = "proyecto_final"
    user = "airflow"
    password = "airflow"
    host = "host.docker.internal"
    port = "5430"

    conn = psycopg2.connect(dbname=dbname,
                            user=user,
                            password=password,
                            host=host,
                            port=port)
    
    cursor = conn.cursor()
    # Define the PostgreSQL table name
    table_name = "prueba_carga_externa"

    # Define the path to the CSV file
    csv_file_path = file_path

    # Define the SQL statement to copy data from the CSV file to the PostgreSQL table
    sql_statement = sql.SQL("COPY {} FROM %s CSV HEADER").format(sql.Identifier(table_name))

    try:
        # Open the CSV file and copy the data to the PostgreSQL table
        with open(csv_file_path, 'r') as f:
            cursor.copy_expert(sql_statement, f)
        
        # Commit the changes
        conn.commit()
        cursor.close()
    except Exception as e:
        # Roll back the transaction in case of an error
        conn.rollback()
        cursor.close()
        conn.close()
        raise e
    finally:
        # Close the connection
        conn.close()



default_args = {
    'owner': 'jDiego',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = "external_load_v01",
    default_args = default_args,
    start_date = datetime(2023, 10, 13),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """
            CREATE TABLE IF NOT EXISTS prueba_carga_externa(
                col_1 character varying,
                col_2 character varying,
                col_3 character varying,
                primary key (col_1, col_2)
                )
        """
    )




    task2 = PythonOperator(
        task_id = 'load_data_to_postgres',
        python_callable = load_data_to_postgres,
    )



    # task2 = PostgresOperator(
    #     task_id = 'insert_into_table',
    #     postgres_conn_id = 'postgres_localhost',
    #     sql = """
    #         INSERT INTO dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
    #     """
    # )

    # task3 = PostgresOperator(
    #     task_id = 'delete_into_table',
    #     postgres_conn_id = 'postgres_localhost',
    #     sql = """
    #         DELETE FROM dag_runs WHERE dt = '{{ ds }}' AND dag_id = '{{ dag.dag_id }}'
    #     """
    # )


    task1 >> task2