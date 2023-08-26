"""
DAG работает с понедельника по субботу
Забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Вывоводит результат работы в любом виде: в логах либо в XCom'е

"""

import logging
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'owner': 'a-mishin-12',
    'poke_interval': 600,
    'catchup': True,
    'start_date': days_ago(int((datetime.now() - datetime.strptime('2022-03-01', '%Y-%m-%d')).days) + 1),
   # 'start_date': datetime(2022, 3, 1),
   #  'end_date': datetime(2022, 3, 14),
    # 'start_date': datetime(2022, 9, 2),
    # 'end_date': datetime(2022, 9, 15)
    'retries': 2,
    'trigger_rule': 'one_success'
}

with DAG("take_table_articles_from_Greenplam",
    schedule_interval='0 12 * * MON,TUE,WED,THU,FRI,SAT', #работает с понедельника по субботу
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mishin-12-lesson-4']
) as dag:


    start = DummyOperator(task_id='start', dag=dag)


    def connect_to_greenplam(execution_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('alex_mishin_cursor')
        day_of_week = datetime.strptime(execution_date, '%Y-%m-%d').weekday() + 1
        cursor.execute(f'SELECT id, heading FROM articles where id = {day_of_week}')  # исполняем sql
        logging.info('we have got id from heading')
        query_res = cursor.fetchall()  # полный результат
        logging.info(str(query_res))

    read_table_from_greenplam = PythonOperator(
        task_id = 'read_table_from_greenplam',
        python_callable = connect_to_greenplam,
        provide_context = True,
        op_kwargs = {'execution_date': '{{ ds }}'}
    )

    start >> read_table_from_greenplam





    # def data_acquisition_status():
    #     logging.info("data acquisition was successful'")
    #
    # data_acqisition = PythonOperator(
    #     task_id = 'data_acqisition',
    #     python_callable = data_acquisition_status
    # )

    # read_table_from_greenplam >> data_acqisition