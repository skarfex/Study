"""
-даг должен работать с понедельника по субботу, но не по воскресеньям
-Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
-Выводить результат работы в любом виде: в логах либо в XCom'е
-Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
#from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta
import pendulum

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

DEFAULT_ARGS = {
    'owner': 'ivan-zaharov',
    'depends_on_past': True,
    'wait_for_downstream': False,
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'trigger_rule':  'all_success',
    'poke_interval': 600,
    'retry_delay': timedelta(minutes=1),
    'retries': 1
}

with DAG("ivan_zaharov_les4_homework",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ivan_zaharov']
         ) as dag:

    dummy = DummyOperator(
        task_id='dummy'
    ) # Dummy operator - пустой таск, никаких действий

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    ) # Bash operator - фиксируем текущий execution date в баше

    def is_not_sunday_func(**kwargs):
        execution_dt = kwargs['execution_dt']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        kwargs['ti'].xcom_push(key='exec_day', value=str(exec_day))             # пушим номер дня недели в другой таск явным методом xcom

        logging.info('-----------------------------')
        logging.info(f'execution_dt = {execution_dt}')                          # вывод в логи execution_dt
        logging.info('exec_day pushed to Xcom = ' + str(exec_day))              # также зафиксируем в логах номер дня недели
        logging.info('-----------------------------')
        return exec_day != 7                                                    # возвращает 'false' если воскресенье

    check_day_is_not_sunday = ShortCircuitOperator(
        task_id='check_day_is_not_sunday',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    ) # ShortCircuit operator - вид Python оператора, передает успех для следующей задачи, если день недели не воскресенье

    def read_greenplum_table_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        exec_day = kwargs['ti'].xcom_pull(task_ids='check_day_is_not_sunday', key='exec_day')    # забираем номер дня недели из xcom

        logging.info('-----------------------------')
        logging.info('exec_day pulled from Xcom = ' + str(exec_day))  # зафиксируем в логах номер дня недели, полученный из Xcom
        logging.info('-----------------------------')

        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')  # исполняем sql
        # query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение

        logging.info('-----------------------------')
        logging.info(f'heading (id = {exec_day}) = {one_string}')                          # зафиксируем в логах заголовок
        logging.info('-----------------------------')

    read_greenplum_table = PythonOperator(
        task_id='read_greenplum_table',
        python_callable=read_greenplum_table_func
    ) # Python Operator - считывает данные из таблицы greenplum

    dummy >> echo_ds >> check_day_is_not_sunday >> read_greenplum_table