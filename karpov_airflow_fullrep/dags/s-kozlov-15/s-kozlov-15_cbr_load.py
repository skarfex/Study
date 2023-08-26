"""
Загрузка курсов валют с CBR в GP
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import locale
import logging
import requests
locale.setlocale(locale.LC_ALL, '')

from airflow.operators.dummy import DummyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook #  хук для работы с GP

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-kozlov-15',
    'poke_interval': 600
}

with DAG("sk_dag_cbr_load",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['sk_cbr_load']
         ) as dag:

    df_rates = pd.DataFrame()

    def if_working_day(exec_dt):
        exec_day = datetime.strptime(exec_dt, '%Y-%m-%d').isoweekday()
        return exec_day in [1,2,3,4,5]

    workday_run = ShortCircuitOperator(
        task_id='workday_run',
        python_callable=if_working_day,
        op_kwargs={'exec_dt': '{{ds}}'}
    )

    def load_rates_from_cbr(**kwargs):
        global df_rates
        tod = datetime.now().strftime("%d%m%Y")
        set_date = f'{tod[:2]}/{tod[2:4]}/{tod[-4:]}'
        logging.info(f'**** ДАТА ДЛЯ ПАРСИНГА: {set_date} ****')
        logging.info(f'>>> Парсим CRB')
        response = requests.get(f'https://cbr.ru/scripts/XML_daily.asp?date_req={set_date}')
        df_rates = pd.read_xml(response.text)
        logging.info('**** LOAD OK ****')
        df_rates = df_rates.astype({'ID': "string", \
                                    'NumCode': "string", \
                                    'CharCode': "string", \
                                    'Nominal': "string", \
                                    'Name': "string", \
                                    'Value': "string"})
        df_rates = df_rates.rename(columns={'ID': 'id', \
                                            'NumCode': 'num_code', \
                                            'CharCode': 'char_code', \
                                            'Nominal': 'nominal', \
                                            'Name': 'name', \
                                            'Value': 'value'})
        logging.info(df_rates.head())
        dt_transfer = df_rates
        return dt_transfer.to_json()

    load_rates = PythonOperator(
        task_id='load_rates',
        python_callable=load_rates_from_cbr,
        dag=dag,
        do_xcom_push = True
    )

    def load_to_gp(ti):
        df_rates = pd.read_json(ti.xcom_pull(task_ids=['load_rates']))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write', schema='students')
        logging.info(df_rates.head(5))
        pg_hook.insert_rows('s_kozlov_15_cbr_data', df_rates)


    write_table = PythonOperator(
        task_id='write_table',
        python_callable=load_to_gp,
        dag=dag
    )
    workday_run >> load_rates >> write_table