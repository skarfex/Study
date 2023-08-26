from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from a_syromyatnikov_14_plugins.a_syromyatnikov_14_ram_location import ASyromyatnikovRamLocationOperator

DEFAULT_ARGS = {
    'owner': 'a_syromyatnikov_14',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 1,
    'start_date': datetime(2022, 11, 14),
    'trigger_rule': 'all_success'

}

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

dag = DAG("a_syromyatnikov_14_ram_location",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=False,
          tags=['a-syromyatnikov-14']
          )


def create_table():
    sql_query = """CREATE TABLE IF NOT EXISTS a_syromyatnikov_14_ram_location
                    (
                        id integer,
                        name text,
                        type text,
                        dimension text,
                        resident_cnt integer
                    );
                    """
    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)


def clear_table():
    sql_query = "TRUNCATE TABLE a_syromyatnikov_14_ram_location"
    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)


table_creation = PythonOperator(
    task_id='table_creation',
    python_callable=create_table,
    dag=dag
)

clearing_table = PythonOperator(
    task_id='clearing_table',
    python_callable=clear_table,
    dag=dag
)

load_location = ASyromyatnikovRamLocationOperator(
    task_id='load_location_info',
    dag=dag
)

table_creation >> clearing_table >> load_location