"""
Rick and Morty Locations to Greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-sarvarov-10',
    'poke_interval': 600
}

with DAG("v-sarvarov-10_ram_location",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-sarvarov-10']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def dataframe_func():
        r = requests.get('https://rickandmortyapi.com/api/location')
        result_json = r.json().get('results')

        res_id_list = []
        res_name_list = []
        res_type_list = []
        res_dimension_list = []
        res_residents_list = []

        for one_loc in result_json:
            res_id_list.append(one_loc.get('id'))
            res_name_list.append(one_loc.get('name'))
            res_type_list.append(one_loc.get('type'))
            res_dimension_list.append(one_loc.get('dimension'))
            res_residents_list.append(len(one_loc.get('residents')))

        res_dataframe = pd.DataFrame(
            {'id': res_id_list,
            'name': res_name_list,
            'type': res_type_list,
            'dimension': res_dimension_list,
            'resident_cnt': res_residents_list,
            })

        res_dataframe_top3 = res_dataframe.sort_values(by='resident_cnt', ascending=False).head(3)
        
        engine = create_engine('postgresql://student:Wrhy96_09iPcreqAS@greenplum.lab.karpov.courses:6432/students')
        res_dataframe_top3.to_sql('v_sarvarov_ram_location', engine, if_exists='replace')

    ram_location = PythonOperator(
        task_id='ram_location',
        python_callable=dataframe_func
    )

    dummy >> ram_location
