from airflow import DAG
import datetime as dt
from airflow.utils.dates import days_ago
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
# hook to connect to greenplum
from airflow.providers.postgres.hooks.postgres import PostgresHook
# configure connection params for this hook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# specify data
DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 3, 1),
    'end_date': dt.datetime(2022, 3, 14),
    'owner': 'u-latynski',
    'poke_interval': 300
}

# schedule from mon to sat
with DAG("u_latynski_task_4_1",
    schedule_interval="0 0 * * MON-SAT",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['u-latynski', 'task_4_1']
) as dag:
    
    def read_articles_fn(**kwargs):
        logging.debug("Running: read_articles_fn")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        ds = kwargs.get('ds')
        logging.debug("DS: {}".format(ds))
        dt_object = dt.datetime.strptime(ds, '%Y-%m-%d')
        day_of_week = dt_object.weekday()+1
        sql = "SELECT heading FROM public.articles WHERE id = {}".format(day_of_week)
        logging.debug("SQL: {}".format(sql))
        cursor.execute(sql)
        one_string = cursor.fetchone()[0]
        logging.info(one_string)
        logging.debug("Running: read_articles_fn - done")
    
    read_article_data = PythonOperator(        
        task_id="read_article_data",
        python_callable=read_articles_fn,
        dag = dag
    )
    
    read_article_data