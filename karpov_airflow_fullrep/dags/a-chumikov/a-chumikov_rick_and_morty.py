""" 5 урок. DAG по API Rick&Morty """

from a_chumikov_plugins.a_chumikov_ram_operator import MaxNLocationsOperator
from a_chumikov_plugins.a_chumikov_create_table_operator import CreateTableOperator
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-chumikov',
    'poke_interval': 500
}

with DAG("a-chumikov_rick_and_morty",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-chumikov']
         ) as my_dag:

    start = DummyOperator(task_id='start')

    create_table = CreateTableOperator(
        task_id='create_table',
        table_name='public.a_chumikov_ram_location',
        columns_types_dict={'id': 'int', 'name': 'text', 'type': 'text',
                            'dimension': 'text', 'resident_cnt': 'int'}
    )

    extract_load_data = MaxNLocationsOperator(
        task_id='extract_load_data',
        api_url='https://rickandmortyapi.com/api/location',
        table_name_to_insert='public.a_chumikov_ram_location',
        top_n=3
    )

start >> create_table >> extract_load_data

my_dag.doc_md = __doc__
