"""
Урок 5
Разработка плагинов
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from e_pavlova_20_plugins.rick_and_morty_locations import top_3_locations

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-pavlova-20',
    'poke_interval': 600
}

with DAG(
     dag_id="pavlova-hw-lesson5",
     schedule_interval='@daily',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['e-pavlova-20']
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    '''
    Таблица e_pavlova_20_ram_location заранее была создана через DBeaver
    '''

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE e_pavlova_20_ram_location;'
    )

    get_top3_locations = top_3_locations(
        task_id='top_3_locations',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start >> truncate_table >> get_top3_locations >> end

