from datetime import datetime as dt
from os.path import basename

from airflow.decorators import dag
from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


OWNER = 'i-dubinskij-11'
DEFAULT_ARGS = {
    'start_date': dt(2022, 3, 1),
    'end_date': dt(2022, 3, 14),
    'owner': OWNER,
    'poke_interval': 600
}


def get_file_name(file_name):
    fn = str(basename(file_name))
    return fn[:fn.rfind('.')]


def get_article(ds: str) -> str:
    day_of_week = dt.strptime(ds, "%Y-%m-%d").weekday()

    if day_of_week == 6:
        raise AirflowSkipException('Today is sunday...')

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {str(day_of_week)}')
    query_res = cursor.fetchall()

    return query_res


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval='@once',
    dag_id=get_file_name(__file__),
    tags=['dubinsky', 'gp']
)
def d_gen():
    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    log_ds = PythonOperator(
        task_id='get_article',
        python_callable=get_article,
        op_kwargs={'ds': echo_ds.output}
    )

    echo_ds >> log_ds


dag = d_gen()
