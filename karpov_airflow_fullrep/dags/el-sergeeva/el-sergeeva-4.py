"""
Даг урок 4

Работает с понедельника по субботу
Ходит в наш GreenPlum.
Используйте соединение 'conn_greenplum'
Получает из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводит результат работы
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""
from airflow import DAG
import logging
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    'start_date':  datetime(2022, 3, 1,3,0,0),
    'end_date': datetime(2022, 3, 14,3,0,0),
    'owner': 'el-sergeeva',
    'poke_interval': 400
}

with DAG("el-sergeeva-4",
    schedule_interval='0 3 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['el-sergeeva']
) as dag:
    def get_wd (**kwargs):
        week_day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        logging.info("Weekday number is " + str(week_day))
        return week_day
    def get_head (**kwargs):
        week_day = kwargs['templates_dict']['week_day']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("get_data")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id ={week_day} limit 1')  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('GET_H'+one_string)
        kwargs['ti'].xcom_push(value=one_string, key='one_string')
    def print_head (**kwargs):
        logging.info('_______________')
        logging.info(kwargs['ti'].xcom_pull(task_ids='get_heading', key='one_string'))
        logging.info('_______________')

    start = DummyOperator(task_id="start")

    get_week_day = PythonOperator(
        task_id='get_week_day',
        python_callable=get_wd,
        provide_context=True
    )
    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_head,
        templates_dict={'week_day': '{{ ti.xcom_pull(task_ids="get_week_day", key="return_value") }}'},
        provide_context=True
    )
    print_heading = PythonOperator(
        task_id='print_heading',
        python_callable=print_head,
        provide_context=True
    )
    end =DummyOperator (task_id = "end")

    start >> get_week_day >> get_heading >> print_heading >> end