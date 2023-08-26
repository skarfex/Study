"""
Забираем данные с greenplum из таблицы articles поле heading пн-сб 
"""
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {    
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'owner': 'm-galitskij-19',
    'poke_interval': 60
}

with DAG("m-galitskij-19_data_gp_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-galitskij-19'],
    catchup=True
) as dag:

    end = DummyOperator(
        task_id='end'        
    )

    def get_data_from_gp_func(**kwargs):
        ds_day = datetime.strptime(kwargs['ds'], "%Y-%m-%d").date()
        v_num_iso_today = ds_day.isoweekday()
        logging.info(f'дата: {ds_day} день номер {v_num_iso_today}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор    
        cursor.execute(f'SELECT heading FROM articles WHERE id = {v_num_iso_today}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат    
        logging.info(f'result is: {query_res}') #записываем в лог

    get_data_from_gp = PythonOperator(
        task_id = 'get_data_from_gp',
        python_callable=get_data_from_gp_func
    )

    def check_weekday_func(**kwargs):     
        ds_day = datetime.strptime(kwargs['ds'], "%Y-%m-%d").date()
        v_num_iso_today = ds_day.isoweekday()
        logging.info(f'дата: {ds_day} день номер {v_num_iso_today}')
        if v_num_iso_today < 7:
            return 'get_data_from_gp'
        else:
            return 'end'

    check_weekday = BranchPythonOperator(
        task_id='check_weekday',
        python_callable=check_weekday_func,
    )

    check_weekday >> [get_data_from_gp, end]