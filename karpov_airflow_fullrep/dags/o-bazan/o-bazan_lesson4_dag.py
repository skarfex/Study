'''
Даг, посвященный заданию 4-го урока из блока "ETL"
Раз в день по всем дням, кроме воскресенья, в 02:00 по Гринвичу (реализация с помощью расписания)
даг выводит в лог из таблицы articles GreenPlum значение поля heading из строки с id,
равным дню недели даты запланированного исполнения дага
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года

В функциях convert_to_week_day_func и get_data_from_greenplum_func реализуем два разных типа push (явный и неявный)
исключительно в целях тренировки (нужно соблюдать единобразие кода)
'''

from airflow import DAG
import logging
import datetime as dt
import pendulum

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'o-bazan',
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc')
}

dag = DAG(
    dag_id = "o-bazan_lesson4_dag",
    schedule_interval='0 2 * * 1-6',
    default_args=DEFAULT_ARGS,
    tags=['lesson4', 'o-bazan']
)

# Преобразование даты исполнения в номер дня недели
def convert_to_week_day_func(**kwargs):
    ds_date = dt.datetime.strptime(kwargs['ds'], '%Y-%m-%d') # преобразование ds к типу данных datetime
    week_day = ds_date.isoweekday()
    return week_day # неявный push в xcom

convert_to_week_day_task = PythonOperator(
    task_id='convert_to_week_day_task',
    python_callable=convert_to_week_day_func,
    provide_context=True,
    dag=dag
)

# Получение данных из GreenPlum
def get_data_from_greenplum_func(**kwargs):
    week_day = kwargs['ti'].xcom_pull(task_ids='convert_to_week_day_task', key='return_value')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализация хука
    conn = pg_hook.get_conn()  # возвращает соединение к базе данных (объект)
    cursor = conn.cursor()  # создание курсора
    cursor.execute(f"SELECT heading FROM articles WHERE id = {week_day}")  # выполнение SQL запроса
    query_res = cursor.fetchall()  # извлечение всего результата из буфера курсора в переменную
    query_res = query_res[0][0]  # получение значения первой колонки первой строки
    cursor.close()
    conn.close()
    kwargs['ti'].xcom_push(key='greenplum_data', value=query_res) # явный push в xcom

get_data_from_greenplum_task = PythonOperator(
    task_id='get_data_from_greenplum_task',
    python_callable=get_data_from_greenplum_func,
    dag=dag
)

# Вывод результатов в лог
def output_result_func(**kwargs):
    week_day = kwargs['ti'].xcom_pull(task_ids='convert_to_week_day_task', key='return_value')
    query_res = kwargs['ti'].xcom_pull(task_ids='get_data_from_greenplum_task', key='greenplum_data')
    logging.info('________________________________________________________________')
    logging.info(f'Weekday: {week_day}')
    logging.info(f'Return value: {query_res}')
    logging.info('________________________________________________________________')

output_result_task = PythonOperator(
    task_id='output_result_task',
    python_callable=output_result_func,
    dag=dag
)

convert_to_week_day_task >> get_data_from_greenplum_task >> output_result_task