'''
Даг забирает из API (https://rickandmortyapi.com/documentation/#location)
топ 3 локации сериала "Rick and Morty" с наибольшим количеством резидентов.
После чего загружает их в GreenPlum ivan_zaharov_ram_location
'''

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
import pendulum

from ivan_zaharov_plugins.ivan_zaharov_ram_top_loc_operator import IvanZaharovRamTopLocationOperator

DEFAULT_ARGS = {
    'owner': 'ivan-zaharov',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 2, tz='utc'),
}

with DAG('ivan_zaharov_les5_homework',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ivan_zaharov']
         ) as dag:

    create_gp_table = PostgresOperator(
        task_id='create_gp_table',
        postgres_conn_id="conn_greenplum_write",
        sql='''
        CREATE TABLE IF NOT EXISTS ivan_zaharov_ram_location (
            id INT,
            name VARCHAR,
            type VARCHAR,
            dimension VARCHAR,
            resident_cnt INT
        );
        '''
    )

    clear_gp_table = PostgresOperator(
        task_id='clear_gp_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE ivan_zaharov_ram_location'
    )

    get_top_3_locations_from_API = IvanZaharovRamTopLocationOperator(
        task_id='get_top_3_locations_from_API'
    )

    write_locations_to_gp_table = PostgresOperator(
        task_id='write_locations_to_gp_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
        INSERT INTO ivan_zaharov_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_3_locations_from_API', key='return_value') }}
        '''
    )

    create_gp_table >> clear_gp_table >> get_top_3_locations_from_API >> write_locations_to_gp_table
