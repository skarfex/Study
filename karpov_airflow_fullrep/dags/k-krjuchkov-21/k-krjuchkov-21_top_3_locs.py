"""
DAG Homework Lab 2
Вывести топ 3 локации из API
Добавить в таблицу greenplum
Предусмотреть защиту от появления дубликатов
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from k_krjuchkov_21_plugins.top_3_ram_locations_operator import RamTop3LocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(4),
    'owner': 'k-krjuchkov-21',
    'poke_interval': 600
}

with DAG("k-krjuchkov-21_top_3_locs",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-krjuchkov-21']
) as dag:

    start = DummyOperator(task_id="start")

    bash_step = BashOperator(
        task_id='bash_step',
        bash_command='echo {{ ts }}',
        trigger_rule='one_success'
    )

    top_locations_step = RamTop3LocationsOperator(
        task_id='top_locations_step',
        trigger_rule='one_success'
    )

    sql = """
            CREATE TABLE IF NOT EXISTS k_krjuchkov_21_ram_location (
                id integer NOT NULL,
                name varchar NULL,
                type varchar NULL,
                dimension varchar NULL,
                resident_cnt integer NULL)
            DISTRIBUTED BY (id);
            """

    create_table_step = PostgresOperator(
        task_id='create_table_step',
        trigger_rule='one_success',
        postgres_conn_id='conn_greenplum_write',
        sql=sql,
        autocommit=True
    )
    def insert_table_step(**kwargs):
        locations = kwargs['ti'].xcom_pull(task_ids='top_locations_step')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write', supports_autocommit=True)
        pg_hook.run("TRUNCATE TABLE k_krjuchkov_21_ram_location")
        pg_hook.run(f"INSERT INTO k_krjuchkov_21_ram_location VALUES {locations};")

    insert_table_step = PythonOperator(
        task_id='insert_table_step',
        python_callable=insert_table_step,
        provide_context=True,
        trigger_rule='one_success'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    start >> bash_step >> end
    start >> top_locations_step >> create_table_step >> insert_table_step >> end