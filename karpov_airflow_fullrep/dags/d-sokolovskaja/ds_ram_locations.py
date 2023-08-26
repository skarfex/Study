from d_sokolovskaja_plugins.d_sokolovskaja_ram_location_operator import DSokolovskajaRamLocationOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2023, 3, 15),
    'owner': 'd-sokolovskaja'
}

with DAG(
        dag_id='ds_ram_locations_dag',
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['dsokolovskaia']
) as dag:
    """pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    create_target_table = PostgresOperator(
        task_id="create_target_table",
        sql="
                CREATE TABLE IF NOT EXISTS students.dsokolovskaja_ram_location (
                id int,
                name text,
                type text,
                dimension text,
                resident_cnt int
                )
                distributed by (id)
                ;
              ",
        postgres_conn_id=conn
    )
    """
    extract_locations_max_residents = DSokolovskajaRamLocationOperator(
        task_id='extract_locations_max_residents'
        )

