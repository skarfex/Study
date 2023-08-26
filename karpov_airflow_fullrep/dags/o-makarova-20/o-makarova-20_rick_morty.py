## o_makarova_20_rick_morty
import pendulum
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from o_makarova_plugins.o_makarova_ram_operator import RMGetPopularLocationOperator

DEFAULT_ARGS = {
    'owner': 'o-makarova-20',
    'start_date': pendulum.datetime(2022, 5, 20, 3, tz="UTC"),
    'poke_interval': 600,
    'retries': 2
    }

TABLE_NAME = 'o_makarova_20_ram_location'

with DAG("o-makarova-20_rick_morty",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['o-makarova-20']
         ) as dag:

    get_data = RMGetPopularLocationOperator(
    task_id="get_data",
    top_n=3,
    table=TABLE_NAME
    )

    truncate_target_table = PostgresOperator(
    task_id="truncate_target_table",
    postgres_conn_id='conn_greenplum_write',
    sql=f'TRUNCATE TABLE {TABLE_NAME}',
    )

    load_data = PostgresOperator(
    task_id="load_data",
    postgres_conn_id='conn_greenplum_write',
    sql="{{ ti.xcom_pull(task_ids='get_data') }}"
    )

get_data >> truncate_target_table >> load_data