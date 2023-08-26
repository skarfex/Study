"""
Выгружаем данные из GreenPlum
"""

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'Romanchuk',
    'poke_interval': 600
}

dag = DAG("r_romanchuk_upload_gp",
          schedule_interval='0 0 * * 1-6 ',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['romanchuk']
          )

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo "Now: {{ ts }}"',
    dag=dag
)



def date_func():
    # получаем номер дня недели
    return datetime.now().weekday() + 1  # monday = 0 but in Greenplum ID starts from 1


get_weekday = PythonOperator(
    task_id='get_weekday',
    python_callable=date_func,
    dag=dag
)


def upload_gp_func(**kwargs):
    # получаем день недели
    ti = kwargs['ti']
    wd = ti.xcom_pull(task_ids='get_weekday')

    # созданм соединение к GP
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()  # и именованный (необязательно) курсор

    # выполняем запрос и получаем результат
    cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    result = ti.xcom_push(value=query_res, key='article')

    # пишем логи
    print('Return week number: ', wd)
    print('Return value: ', query_res)
    print('Return value: ', result)


upload_gp = PythonOperator(
    task_id='upload_gp',
    python_callable=upload_gp_func,
    provide_context=True,
    dag=dag
)

echo_ds >> get_weekday >> upload_gp
