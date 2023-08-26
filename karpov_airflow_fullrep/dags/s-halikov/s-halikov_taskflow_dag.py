'''
Забираем из Greenplum данные по условию дня
'''

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
# from airflow.utils.dates import days_ago
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import datetime as dt
import logging


DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 14),
    'owner': 's-halikov',
    'poke_interval': 300
}

@dag(
    dag_id='s-halikov_taskflow_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 * * MON-SAT',
    tags=['salavat'],
    max_active_runs=1
)
def taskflow_dag():
    def get_gp_data_func():
        logging.info('get_gp_data started')
        context = get_current_context()
        ds = context['ds']
        week_day = dt.datetime.strptime(ds, '%Y-%m-%d').weekday() + 1
        logging.info('week_day = {}'.format(week_day))

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        logging.info('connection getted')
        cursor = conn.cursor("named_cursor_name")
        logging.info('cursor getted')
        cursor.execute("select heading from articles where id = {}".format(week_day))
        logging.info('cursor executed')
        result = cursor.fetchall()
        return result

    @task
    def get_gp_data():
        '''
        #### Extract task
        Extract data from DB
        '''
        return get_gp_data_func()

    @task
    def log_result(data):
        '''
        #### Load task
        Logging data
        '''
        logging.info('-------------')
        # logging.info(str.replace(data, '\n', ' '))
        logging.info(data)
        logging.info('-------------')

    data = get_gp_data()
    log_result(data)

s_halikov_taskflow_dag = taskflow_dag()
