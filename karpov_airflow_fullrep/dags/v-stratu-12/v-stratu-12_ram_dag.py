"""
Даг для записи ТОП-3 локаций из Rick and Morty API в Greenplum ...
"""
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from v_stratu_12_plugins.v_stratu_12_top_locations_operator import VStratuRamTopLocationsOperator

from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'v-stratu-12',
    'poke_interval': 60
}
with DAG("v-stratu-12_ram_dag",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-stratu-12']
         ) as dag:

    begin = DummyOperator(task_id='begin')

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
        locations.sort(reverse=True, key=lambda x: x['resident_cnt'])
        kwargs['ti'].xcom_push(value=locations[:3], key='locations')

    def load_top3_locations_to_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE if not exists public.v_stratu_12_ram_location (
                id int4 NOT NULL,
                "name" varchar NULL,
                "type" varchar NULL,
                dimension varchar NULL,
                resident_cnt int4 NULL,
                CONSTRAINT v_stratu_12_ram_location_pkey PRIMARY KEY (id)
            )
            DISTRIBUTED BY (id);
        ''')

        cursor.execute('TRUNCATE TABLE public.v_stratu_12_ram_location')

        locations = kwargs['ti'].xcom_pull(task_ids='extract_top3_locations_2', key='return_value')
        rows = ', '.join(f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})" for loc in locations)
        cursor.execute(f'''
            INSERT INTO public.v_stratu_12_ram_location(id, name, type, dimension, resident_cnt)
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

    extract_top3_locations_2 = VStratuRamTopLocationsOperator(
        task_id='extract_top3_locations_2',
        top_count=4
    )

    begin >> extract_top3_locations_2 >> load_top3_locations_to_gp