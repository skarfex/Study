import logging

from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'catchup': True,
    'poke_interval': 30,
    'owner': 'n_vinogreev'
}

dag = DAG(
    dag_id='n_vinogreev',
    schedule_interval='0 10 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n_vinogreev']
)


# Функции
def get_gp_data_func(ds, execution_date):
    ds = {ds}.pop()  # получаем дату, за которую запускаем даг
    date_format = '%Y-%m-%d'  # задаем формат даты, из которого будет преобразовывать строку ds
    day_of_week = datetime.strptime(ds, date_format).weekday() + 1  # преобразуем ds к дате
    sql = f'select heading from articles where id = { day_of_week }'  # генерируем sql
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берем из него соединение
    cursor = conn.cursor()
    cursor.execute(sql)  # исполняем sql
    # query_res = cursor.fetchall() # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось одно значение

    # logging.info(query_res)
    logging.info(one_string)


dummy_operator = DummyOperator(task_id='Start', dag=dag)

get_gp_data_operator = PythonOperator(
    task_id='get_gp_data',
    python_callable=get_gp_data_func,
    dag=dag
)

dummy_operator >> get_gp_data_operator
