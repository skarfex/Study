"""
Ищем Топ 3 заселенных локации в Рик и Морти.
Airflow DAG. Lesson 4.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from s_gerasimov_17_plugins.s_gerasimov_17_ram_operator import SGerasimovTop3LocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-gerasimov-17',
    'poke_interval': 600,
    'retries': 3
}

with DAG("s-gerasimov-17-lesson5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-gerasimov-17']
         ) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")

    def get_page_count(url):
        r = requests.get(url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def extract_top3_locations_func(**kwargs):
        url = 'https://rickandmortyapi.com/api/location?page={page_number}'
        locations = []
        for page_number in range(1, get_page_count(url.format(page_number=1))+1):
            r = requests.get(url.format(page_number=page_number))
            if r.status_code == 200:
                locations_on_page = r.json().get('results')
                locations.extend(
                    [dict({'id': loc['id'], 'name': loc['name'], 'type': loc['type'], 'dimension': loc['dimension'], 'resident_cnt': len(loc['residents'])})
                                  for loc in locations_on_page]
                )
            else:
                logging.error('Something bad happened. Check logs.')
        locations.sort(reverse=True, key=lambda x: x['resident_cnt'])
        kwargs['ti'].xcom_push(value=locations[:3], key='locations')

    def load_top3_locations_to_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS public.s_gerasimov_17_lesson5 (
                id int4 NOT NULL,
                "name" varchar NULL,
                "type" varchar NULL,
                dimension varchar NULL,
                resident_cnt int4 NULL,
                CONSTRAINT s_gerasimov_17_lesson5_pkey PRIMARY KEY (id)
            )
            DISTRIBUTED BY (id);
        ''')

        cursor.execute('TRUNCATE TABLE public.s_gerasimov_17_lesson5')

        locations = kwargs['ti'].xcom_pull(task_ids='extract_top3_locations_2', key='return_value')
        rows = ', '.join(f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})" for loc in locations)
        cursor.execute(f'''
            INSERT INTO public.s_gerasimov_17_lesson5 (id, name, type, dimension, resident_cnt)
            VALUES
            {rows}
        ''')

        conn.commit()

    extract_top3_locations = PythonOperator(
        task_id='extract_top3_locations',
        python_callable=extract_top3_locations_func,
        provide_context=True
    )

    load_top3_locations_to_gp = PythonOperator(
        task_id='load_top3_locations_to_gp',
        python_callable=load_top3_locations_to_gp_func,
        provide_context=True
    )

    extract_top3_locations_2 = SGerasimovTop3LocationOperator(
        task_id='extract_top3_locations_2',
        top_count=3
    )

    dummy_end = DummyOperator(task_id="dummy_end")

    dummy_start >> extract_top3_locations_2 >> load_top3_locations_to_gp >> dummy_end

