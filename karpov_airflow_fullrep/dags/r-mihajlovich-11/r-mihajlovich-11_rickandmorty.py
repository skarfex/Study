"""
RMA top-3 locations
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
import requests
import json

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'r-mihajlovich-11',
    'poke_interval': 600,
    'start_date':days_ago(2)
}

with DAG("r-mihajlovich-11_ram_location",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          catchup= True,
          tags=['r-mihajlovich-11']
          ) as dag:


    def gather_data_func():
        logging.info("Started data gather")
        data=requests.get("https://rickandmortyapi.com/api/location").text
        data=json.loads(data)
        location_residents=[]
        for i in range(len(data['results'])):
            data['results'][i]['resident_cnt']=len(data['results'][i]['residents'])
            list_keys=list(data['results'][i].keys())
            print(list_keys)
            for key in list_keys:
                if key not in ['resident_cnt','id','name','type','dimension']:
                    data['results'][i].pop(key,None)
        data['results'].sort(key=lambda x: x['resident_cnt'],reverse=True)
        top_3_locations=data['results'][:3]
        return [tuple(location.values()) for location in top_3_locations]


    gather_api = PythonOperator(
        task_id='get_top3_loc',
        python_callable=gather_data_func,
        provide_context=True,
        dag=dag,
    )
    
    def write_data_func(**kwargs):
        logging.info("Started writing")
        pg_hook=PostgresHook(postgres_conn_id='conn_greenplum_write')
        #Чистим таблицу
        pg_hook.run("truncate r_mihajlovich_11_ram_location")
        top_3_locations=kwargs['ti'].xcom_pull(task_ids='get_top3_loc')
        logging.info(f"Rows to write: \n {top_3_locations}")
        pg_hook.insert_rows(table='r_mihajlovich_11_ram_location',rows=top_3_locations)
        

        
    write_postgres = PythonOperator(
        task_id='write_pg',
        python_callable=write_data_func,
        dag=dag
    )    

    gather_api >> write_postgres