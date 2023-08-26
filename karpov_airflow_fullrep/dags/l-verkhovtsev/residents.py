import logging
import pandas as pd

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import timedelta, days_ago
from airflow import DAG
from l_verkhovtsev_plugins.l_verkhovtsev_residents import VerkhovtsevRAMLocations

DEFAULT_ARGS = {
    'owner': 'verkhovtsev',
    'wait_for_downstream': False,
    'start_date': days_ago(7),
    'retries': 3,
    'retry_delay': timedelta(seconds=20),
    'priority_weight': 10,
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule':  'all_success'
}

with DAG(
    dag_id="ram_verkhovtsev",
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    max_active_runs=10,
    tags=["verkhovtsev", "ram-dag"],
    ) as dag:

    TOP_K = 3
    starter = BashOperator(task_id="ram_starter",
                           bash_command='echo "Rick and Morty exploration started at {{ ds }}"'
                           )

    """
    We expect only top-3 results (aka 3 rows in the table), 
    so truncate table and reload values is okay
    """
    table_name = "l_verkhovtsev_ram_location"
    temp_location = "/tmp/verkhovtsev_top.csv"

    def create_or_truncate_table():
        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        query = f"""
                   CREATE TABLE IF NOT EXISTS {table_name} (
                       id int, 
                       name varchar(150), 
                       type varchar(50), 
                       dimension varchar(150), 
                       resident_cnt int);
                   TRUNCATE TABLE {table_name};
                   """

        cursor.execute(query)
        conn.commit()
        conn.close()
        logging.info(f"{table_name} created or truncated.")

    create_or_truncate = PythonOperator(
        task_id="creation_or_truncation",
        python_callable=create_or_truncate_table
    )

    get_top_3 = VerkhovtsevRAMLocations(
        task_id="get_top_3",
        top_k=TOP_K
    )

    def push_to_table_func(**content):
        topk_result = content['ti'].xcom_pull(key=f"verkhovtsev_ram_top{TOP_K}")
        logging.info(f'Top k content: {topk_result}')

        table_content = pd.DataFrame.from_dict(topk_result)

        table_content[['id','resident_cnt']] = table_content[['id', 'resident_cnt']].astype(int)
        table_content = table_content.sort_values(by="resident_cnt", ascending=False)
        table_content.to_csv(temp_location, index=False)

        logging.info(table_content)

        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert(f"COPY {table_name} FROM STDIN DELIMITER ',' HEADER", temp_location)

        logging.info('Added successfully!')


    push_to_table = PythonOperator(
        task_id="push_to_table",
        python_callable=push_to_table_func
    )

starter >> create_or_truncate >> get_top_3 >> push_to_table