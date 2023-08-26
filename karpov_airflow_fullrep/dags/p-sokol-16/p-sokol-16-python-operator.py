from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.sensors.weekday_sensor import DayOfWeekSensor
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'p-sokol-16',
    'poke_interval': 600
}

dag = DAG("p-sokol-16-python-operator",
          default_args=DEFAULT_ARGS,
          schedule_interval="@daily",
          tags=['p-sokol-16']
          )


weekend_check = DayOfWeekSensor(
    task_id='weekday_check_task',
    week_day={'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday'},
    use_task_execution_day=True,
    dag=dag)


def select_articles_func(wkd):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {wkd}')  # исполняем sql
    query_res = cursor.fetchall()
    logging.info(query_res)


select_articles = PythonOperator(
    task_id='select_articles',
    op_args=['{{macros.ds_format(ds, "%Y-%m-%d", "%w")}}'],
    python_callable=select_articles_func,
    dag=dag)

weekend_check >> select_articles
