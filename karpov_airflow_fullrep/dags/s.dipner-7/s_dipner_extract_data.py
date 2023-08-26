"""
Период работы: с 1 марта 2022 года по 14 марта 2022 года
date1 = dt(year=2022, month=3, day=1)
date2 = dt(year=2022, month=3, day=14)

—— Техническое задание ——
1. DAG должен работать с Понедельника по Субботу (НЕ включать Воскресенье)
2. Забирать из таблицы articles значение поля heading из строки с id, 
равным дню недели ds (понедельник=1, вторник=2, ...)
3. Выводить результат работы в любом виде:
 3.1 Логов
 3.2 XCom'а

—— Инструментарий ——
1. Поднятая база данных GreenPlum (conn_id="conn_greenplum")

—— Подсказки ——
1. Подключение к GP для считывания данных можно реализовать 
через PythonOperator с PostgresHook внутри
2. Проверку на Воскресенье можно реализовать с помощью:
 2.1 Расписания
 2.2 Операторов ветвления

"""

# Python
import logging
from datetime import datetime as dt

# Airflow
from airflow import DAG
# —— Operators ——
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
# —— Hooks ——
from airflow.hooks.postgres_hook import PostgresHook

# =====================================
#               Functions
# =====================================


def load_data_from_GP(**kwargs):
    """
    1. Get information about DAG run
    2. Get weekday + 1, therefore zero reference
    3. Fetch data by weekday filter
    4. Logging all information

    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html
    get_current_context() - Obtain the execution context for the currently executing operator without
    """
    # Get weekday
    context = get_current_context()
    weekday = context["execution_date"].weekday()+1

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') #Initialize the hook
    conn = pg_hook.get_conn() #Take a connection from it
    cursor = conn.cursor("atricles_data_fetch")
    logging.info(str(weekday))
    if weekday < 7 :
        cursor.execute(f'SELECT heading FROM articles WHERE id = {str(weekday)}') #Execute sql with paramert
        logging.info(f'SELECT heading FROM articles WHERE id = {str(weekday)}')
        one_string = cursor.fetchone()[0] #Returned a single value
        kwargs['ti'].xcom_push(value=one_string, key='value') #Sending result query in the xcom
        logging.info(one_string)


DEFAULT_ARGS = {
    'start_date': dt(year=2022, month=3, day=1),
    'end_date' : dt(year=2022, month=3, day=14),
    'owner': 's.dipner-7'
}

dag = DAG("sdipner_load_gp",
        schedule_interval="0 0 * * 1-6",
        default_args=DEFAULT_ARGS,
        catchup=True,
        max_active_runs=1,
        tags=['karpov', 's.dipner-7']
        )

load_data = PythonOperator(
    task_id='load_data_from_GP',
    python_callable=load_data_from_GP,
    dag=dag
)

load_data
