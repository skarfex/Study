"""
Даг обращается к API rickandmortyapi.com и находит топ 3 локации
с наибольшим количеством резидентов и отправляет данные в GreenPlum
"""
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from n_kalimanov_19_plugins.n_kalimanov_rickandmorty_operator import NkalimanovLocationsTopOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-kalimanov-19',
    'poke_interval': 600,
}

TABLE_NAME = 'n_kalimanov_19_ram_location'


with DAG("n-kalimanov_rick_and_morty",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n-kalimanov-19'],
) as dag:

    exists_table = f'''
        SELECT 1
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = 'public'
        AND    c.relname = '{TABLE_NAME}'
        AND    c.relkind = 'r';
    '''

    create_table_querry = f'''
    Create table {TABLE_NAME}(
            id int PRIMARY KEY,
            name varchar,
            type varchar,
            dimension varchar,
            residents_cnt int
    )
    DISTRIBUTED BY (id);
    '''

    truncate_table_querry = f'Truncate {TABLE_NAME}'


    def load_csv_to_gp_func(**kwargs):
        tmp_csv = kwargs['templates_dict']['tmp_csv']
        logging.info(f' tmp_cav: {tmp_csv}')
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert(f"COPY {TABLE_NAME} FROM STDIN DELIMITER ','", f"{tmp_csv}")

    def create_or_truncate_table():
        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(f'Запрос таблицы {exists_table}')
        cursor.execute(exists_table)
        query_res = cursor.fetchone()
        conn.close()

        logging.info(f'Ответ {query_res}')
        if query_res:
            return 'truncate_table'
        return 'create_table'


    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        autocommit=True,
        sql=create_table_querry,
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        autocommit=True,
        sql=truncate_table_querry,
    )

    branching = BranchPythonOperator(
        task_id='create_or_truncate_table',
        python_callable=create_or_truncate_table,
    )

    top3_locations = NkalimanovLocationsTopOperator(
        task_id='top3_locations',
        trigger_rule='one_success',
    )

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        templates_dict={'tmp_csv': '{{ ti.xcom_pull(task_ids="top3_locations", key="return_value") }}'}
    )

    end_task = DummyOperator(task_id='end_task')

    branching >> [create_table, truncate_table] >> top3_locations
    top3_locations >> load_csv_to_gp >> end_task
