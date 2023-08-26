"""
Даг для выгрузки заголовков из таблицы в GP
"""

from airflow.decorators import dag, task
from datetime import datetime
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'owner': 'd-jurayeu',
    'start_date': datetime(2022, 3, 1, 3),
    'end_date': datetime(2022, 3, 14, 3),
}

@dag(
    "d-jurayeu-4",
    schedule_interval='0 3 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-jurayeu']
)
def d_jurayeu_taskflow():

    @task
    def get_dow_id(**kwargs):
        ds = kwargs["ds"]
        converted_ds = datetime.strptime(ds, '%Y-%m-%d')
        ds_id = converted_ds.weekday() + 1
        return ds_id

    @task
    def get_heading_from_gp(dow_id):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {dow_id}')
        query_res = cursor.fetchall()
        if len(query_res) > 1:
            return [res[0] for res in query_res]
        else:
            return query_res[0][0]

    @task
    def print_logs(query_result):
        logging.info('--------------')
        logging.info(query_result)
        logging.info('--------------')

    print_logs(get_heading_from_gp(get_dow_id()))


d_jurayeu_taskflow_dag = d_jurayeu_taskflow()
