"""
Загружаем данные из API Рика и Морти
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests
import pandas as pd

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'al-a-21'
}

with DAG("al-a-21-lesson-5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['al-a-21']
         ) as dag:


    def get_ram_data():
        request = requests.request(method="GET", url="https://rickandmortyapi.com/api/location")
        page_count = request.json()["info"]["pages"]
        results_df = pd.DataFrame()
        for page in range(page_count):
            request_page = pd.DataFrame(
                requests.request(method="GET", url=f"https://rickandmortyapi.com/api/location?page={page + 1}").json()[
                    "results"])
            results_df = pd.DataFrame(request_page) if results_df.empty else pd.concat([results_df, request_page])

        results_df["resident_cnt"] = results_df["residents"].apply(len)
        results_df = results_df.nlargest(3, "resident_cnt")[["id", "name", "type", "resident_cnt"]]
        logging.info('Top 3 locations in Rick and Morty universe is saved in temp location')
        return results_df

    def insert_into_gp(**context):
        # Retrieve the DataFrame from XCom
        df = context['ti'].xcom_pull(task_ids='get_top_ram_locations')
        # Connect to the GP database
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # initialize hook
        conn = pg_hook.get_conn()  # connecting
        cursor = conn.cursor()
        for index, row in df.iterrows():
            query_1 = f"DELETE FROM al_a_21_ram_location WHERE id = {row['id']};"
            cursor.execute(query_1)
            query_2 = "INSERT INTO al_a_21_ram_location (id, name, type, resident_cnt) VALUES (%s, %s, %s, %s)"
            values = (row['id'], row['name'], row['type'], row['resident_cnt'])
            cursor.execute(query_2, values)
        # Commit and close the database connection
        conn.commit()
        conn.close()

    start = DummyOperator(task_id='start')

    get_top_ram_locations = PythonOperator(
        task_id='get_top_ram_locations',
        python_callable=get_ram_data
    )

    insert_into_gp = PythonOperator(
        task_id='insert_into_gp',
        python_callable=insert_into_gp
    )

    end = DummyOperator(task_id='end')

    start >> get_top_ram_locations >> insert_into_gp >> end