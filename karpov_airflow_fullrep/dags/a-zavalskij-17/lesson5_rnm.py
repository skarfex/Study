"""
Скачиваем с сайта rickandmortyapi.com информацию и загружаем ее в GP
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import requests

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-zavalskij-17',
    'poke_interval': 600,
}


with DAG(
    "a-zavalskij-17-rnm",
    schedule_interval='0 0 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-zavalskij-17'],
) as dag:
    start = DummyOperator(task_id='start')

    def parse_info(**kwargs):
        data = []
        for i in range(1, 8):
            response = requests.get(
                f'https://rickandmortyapi.com/api/location?page={i}'
            )
            for res in response.json().get('results', []):
                item = {
                    'resident_cnt': len(res.get('residents', [])),
                    'id': res.get('id'),
                    'name': res.get('name'),
                    'type': res.get('type'),
                    'dimension': res.get('dimension'),
                }
                data.append(item)

        sorted_data = sorted(data, key=lambda d: d['resident_cnt'], reverse=True)
        kwargs['ti'].xcom_push(value=sorted_data[:3], key='a-zavalskiy-17-locations')

    parse_info_and_push_to_xcom = PythonOperator(
        task_id='push_rnm_info_a-zavalskiy-17',
        python_callable=parse_info,
        provide_context=True,
    )

    def insert_locations(schema, table, **kwargs):
        pg_hook = PostgresHook('conn_greenplum_write')
        locations = kwargs['ti'].xcom_pull(
            task_ids='push_rnm_info_a-zavalskiy-17', key='a-zavalskiy-17-locations'
        )
        for location in locations:
            pg_hook.run(
                sql=(
                f'INSERT INTO {schema}."{table}"'
                f'VALUES ({location["id"]}, \'{location["name"]}\', \'{location["type"]}\', \'{location["dimension"]}\', {location["resident_cnt"]}'
                f');'
                )
            )

    insert_rows = PythonOperator(
        task_id='insert_rows_a-zavalskiy-17',
        python_callable=insert_locations,
        op_args=['public', 'a-zavalskij-17_ram_location'],
        provide_context=True,
    )

    start >> parse_info_and_push_to_xcom >> insert_rows
