"""
Рик и плохой Морти
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator

from v_neverovskij_plugins.v_neverovskij_rick_and_bad_morty import VladNeverovskijRickBadMortyOperator


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 10, 1, tz="UTC"),
    'owner': 'v-neverovskij',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': True
}


with DAG("v-neverovskij-rick-and-bad-morty",
         schedule_interval='0 12 * * *',
         default_args=DEFAULT_ARGS,
         tags=['v-neverovskij']
         ) as dag:

    task1 = PostgresOperator(
        task_id='create_ram_location_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS v_neverovskij_ram_location(
                id int, 
                name varchar, 
                type varchar, 
                dimension varchar, 
                resident_cnt int);
            TRUNCATE TABLE v_neverovskij_ram_location;
            """
    )

    task2 = VladNeverovskijRickBadMortyOperator(
        task_id='RickAndBadMortTOP3locations',
    )

    task3 = BashOperator(
        task_id='jobs_done',
        bash_command='echo Jobs done! Hell yeah!'
    )


    task1 >> task2 >> task3
