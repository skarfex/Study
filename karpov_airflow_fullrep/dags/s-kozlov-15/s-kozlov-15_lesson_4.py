"""
Урок 4 - принты из базы
"""
from airflow import DAG
from datetime import datetime
import locale
import logging
import pendulum
locale.setlocale(locale.LC_ALL, '')


from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook #  хук для работы с GP

DEFAULT_ARGS = {
    'owner': 's-kozlov-15',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'poke_interval': 60
}

with DAG("sk_dag_lesson_4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['sk_lesson_4']
         ) as dag:


    def if_working_day(ds):
        exec_day = datetime.strptime(ds, "%Y-%m-%d").isoweekday()
        if exec_day in [1,2,3,4,5,6]:
            return 'read_table'
        else:
            return 'sunday_task'

    weekday_branch = BranchPythonOperator(
        task_id='weekday_branch',
        python_callable=if_working_day,
        dag=dag
    )

    def load_from_gp(ds):
        exec_day = datetime.strptime(ds, "%Y-%m-%d").isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и курсор
        sql_select = f'SELECT heading FROM articles WHERE id = {exec_day}'
        cursor.execute(sql_select)  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('*****  DATABASE STRING ****')
        logging.info(one_string)
        logging.info('*********')
        logging.info(f'WEEKDAY: {exec_day}')
        logging.info('*********')


    read_table = PythonOperator(
        task_id='read_table',
        python_callable=load_from_gp,
        dag=dag
    )

    def its_sunday():
        print("It's Sunday! Relax and pass")

    sunday_task = PythonOperator(
        task_id='sunday_task',
        python_callable=its_sunday
    )


    weekday_branch >> [read_table, sunday_task]