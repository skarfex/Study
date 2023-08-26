from a_muromtsev_12_plugins.a_muromtsev_12_rick_and_morty_operator import AMuromtsev12Top3Locations
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

import logging
import requests

DEFAULT_ARGS = {
    'owner': 'a-muromtsev-12',
    'start_date': days_ago(2),
    'retries': 3,
    'poke_interval': 600
}

with DAG(
        dag_id="a-muromtsev-12.rick_and_morty_dag",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-muromtsev-12']
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    top_3_locations = AMuromtsev12Top3Locations(
        task_id="top_3_locations",
    )

    def get_data_from_locations_func(ti):
        base_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        query = []
        locations = ti.xcom_pull(task_ids=['top_3_locations'])[0]
        for location in locations:
            r = requests.get(base_url.format(pg=str(location)))
            if r.status_code == 200:
                logging.info(f"GOT location {location} data")
                results = r.json().get('results')[0]
                data = (
                    results.get('id'),
                    results.get('name'),
                    results.get('type'),
                    results.get('dimension'),
                    len(results.get('residents'))
                )
                query.append(data)
        return query


    get_data_from_locations = PythonOperator(
        task_id='get_data_from_locations',
        python_callable=get_data_from_locations_func
    )


    def load_data_in_grennplum_func(ti):
        query = ti.xcom_pull(task_ids=['get_data_from_locations'])[0]
        logging.info(f'query is {query}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(f"""create table if not exists public.a_muromtsev_12_ram_location (
	                        id int4,
	                        name varchar(50),
	                        type varchar(50),
	                        dimension varchar(50),
            	            resident_cnt int4
                        );
                 """)
        pg_hook.run(f"""insert into public.a_muromtsev_12_ram_location
                        select * from(
                        values {query[0]},
                               {query[1]},
                               {query[2]}
                                    ) as query
                        where not exists (select * from public.a_muromtsev_12_ram_location);      
                    """, False)
        return


    load_data_in_grennplum = PythonOperator(
        task_id='load_data_in_grennplum',
        python_callable=load_data_in_grennplum_func
    )

    start >> top_3_locations >> get_data_from_locations >> load_data_in_grennplum
