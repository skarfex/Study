"""
Call Richard'n'Mortimer API for location
Using Event Loop with Event trigger for I/O bound reduction.
TODO: 1) add 'run_in_executor' leter or concurrent.futures Pool ('on-the-fly' generate tasks not possible)
      2) add global DB connection and handling 
        (i.e. start_task[open conn], end_tasl[close conn]) for reduce reconnecting overhead
      3) add decorators for event.set() and await event.wait() generating
      4) add generating from JSON
      5) maybe add generator of SQL UPSERTS
      
      P.S. there is no 'aiohttp' on server...
"""

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from d_krasovskij_14_plugins.simple_api_caller import SimpleApiCallerLoader
from d_krasovskij_14_plugins.sync_caller import sync_call
from d_krasovskij_14_plugins.utils import filter_result
from d_krasovskij_14_plugins.db_utils import (
    prepare_values, 
    generate_upsert_statement, 
    create_table_sql, 
    check_table_sql
)

import logging
from datetime import datetime, timedelta
from typing import List


DEFAULT_ARGS = {
    'owner': 'd-krasovskij-14',
    'poke_interval': 300,
	'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 1),
    'end_date': datetime(2022, 11, 21),
	'schedule_interval': '@once',
    # 'schedule_interval': '*/15 * * * 1-6',
}


CONNECTION_ID = 'conn_greenplum_write'

TASK_POSTFIX = 'ram_location'
# ROOTNAME = 'd_krasovskij_14'
ROOTNAME = 'krasovsky_da'
TABLENAME = f'{ROOTNAME}_{TASK_POSTFIX}'
# INDEX = f"{TABLENAME}_idx"

ENDPOINT = 'https://rickandmortyapi.com/api'
PATH = 'location'
PAGINATION = True
N_PAGES = None


### aux functions
def create_table_func():
    """creates ETL table and index if not exist"""
    pg_hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    pg_hook.run(create_table_sql, False)
    result = pg_hook.run(check_table_sql(TABLENAME), False)
    logging.info(result)


def insert_to_table(records:List[dict]):
    records = prepare_values(records, in_braces=False)
    pg_hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
    stmt = generate_upsert_statement(TABLENAME, records)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                result = cur.execute(stmt)
                logging.info(f"UPSERT result: {result}")
            except Exception:
                raise AirflowException(f'ERROR on UPSERT. VALUES:\n{stmt}')


def call_and_proc_data_func():
    result = sync_call(endpoint=ENDPOINT, path=PATH, pagination=PAGINATION, n_pages=N_PAGES)

    top_three_populated = filter_result(result, n=3, by='residents_cnt', reverse=True)

    logging.info(f'VALUES ON LOAD\n{top_three_populated}')

    result = insert_to_table(top_three_populated)
    logging.info(f"Data loaded. Status: {result}")


with DAG(dag_id="d-krasovskij-14_lesson-5",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-krasovskij-14', 'lesson-5']) as dag:

    start = DummyOperator(task_id='start')

    create_table_if_not_ex = PostgresOperator(
        task_id='create_table',
        sql = create_table_sql(TABLENAME),
        autocommit=True,
        postgres_conn_id=CONNECTION_ID
    )

    # call_and_proc_data = PythonOperator(
	# 	task_id='call_and_proc_data',
    #     python_callable=call_and_proc_data_func 
	# )
    
    call_and_proc_data = SimpleApiCallerLoader(
		task_id='call_and_proc_data',
        db_connection=CONNECTION_ID,
        tablename=TABLENAME,
        endpoint=ENDPOINT,
        path=PATH,
        fetch_n=3,
        fetch_by='residents_cnt', ### TODO: do bypass and logic of calculated aggregate/metrics from config
        desc=True,
        pagination=PAGINATION,
        pages=N_PAGES,
        async_caller=False
    )

    end = DummyOperator(task_id='end')

    start >> create_table_if_not_ex >> call_and_proc_data >> end
