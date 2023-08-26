"""
Урок 5 - Rick&Morty
"""
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
import locale
import logging
import pandas as pd
import requests
from io import StringIO
import json
locale.setlocale(locale.LC_ALL, '')


from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook #  хук для работы с GP

df = pd.DataFrame()

def get_pages_cnt():
    api_url = 'https://rickandmortyapi.com/api/location'
    req = requests.get(api_url)
    return req.json().get('info').get('pages')

def get_page(page_number):
    api_url = f'https://rickandmortyapi.com/api/location/?page={page_number}'
    return requests.get(api_url)

def get_all_data():
    global df
    for i in range(1, get_pages_cnt()+1):
        req = get_page(i)
        df_tmp = pd.json_normalize(req.json().get('results') )
        df = pd.concat([df, df_tmp])
    df['resident_cnt'] = df.residents.apply(lambda x: len(x))
    df = df.sort_values(by='resident_cnt', ascending=False).reset_index()[['id', 'name', 'type', 'dimension', 'resident_cnt']].loc[:2]
    logging.info(df.head(5))
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write', schema='students')
    logging.info(df.head(5))

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep='\t', header=False)

    pg_hook.bulk_load(table='s_kozlov_15_ram_location', tmp_file=csv_buffer)

    #pg_hook.insert_rows('s_kozlov_15_ram_location', df)



DEFAULT_ARGS = {
    'owner': 's-kozlov-15',
    'start_date': days_ago(1),
    'poke_interval': 60
}

with DAG("sk_dag_lesson_5",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['sk_lesson_5_rick']
         ) as dag:




    def load_to_gp():
        global df
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write', schema='students')
        logging.info(df.head(5))
        pg_hook.insert_rows('s_kozlov_15_ram_location', df)

    def truncate_table():
        truncate_sql = "truncate table s_kozlov_15_ram_location"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(truncate_sql, False)

    trunc_table = PythonOperator(
        task_id='trunc_table',
        python_callable=truncate_table,
        dag=dag
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_all_data,
        dag=dag
    )


    trunc_table >> get_data