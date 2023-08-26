"""
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

from k_shirjaev_plugins.k_shirjaev_ram_top_locations_operator import KshirjaevRamTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'k-shirjaev'
}

with DAG('k-shirjaev-5lp3',
         schedule_interval='@once',
         max_active_runs=1,
         default_args=DEFAULT_ARGS,
         tags=['konshi']
         ) as dag:

    prepare_gp_table = PostgresOperator(
        task_id="prepare_gp_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            CREATE TABLE IF NOT EXISTS k_shirjaev_ram_location(
                id INT,
                name VARCHAR,
                type VARCHAR,
                dimension VARCHAR,
                resident_cnt INT
            );
            TRUNCATE TABLE k_shirjaev_ram_location;
            """,
    )

    extract_from_api = KshirjaevRamTopLocationsOperator(task_id="extract_from_api")

    load_to_gp = PostgresOperator(
        task_id="load_to_gp",
        postgres_conn_id="conn_greenplum_write",
        sql="INSERT INTO k_shirjaev_ram_location VALUES {{ ti.xcom_pull(task_ids='extract_from_api') }}"
    )

    prepare_gp_table >> extract_from_api >> load_to_gp
