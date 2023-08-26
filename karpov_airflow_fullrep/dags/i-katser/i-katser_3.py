"""
Л5
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from i_katser_plugins.i_katser_top_n_locations_operatop import IKatserTopNLocationsOperator
import json


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-katser',
    'poke_interval': 600,
    'trigger_rule': 'all_success'
}

with DAG("DAG_i-katser_3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-katser']
         ) as dag:


    dummy = DummyOperator(task_id="dummy")


    def create_table():
        create = """CREATE TABLE IF NOT EXISTS i_katser_ram_location (
                        id int PRIMARY KEY,
                        name varchar NOT NULL,
                        type varchar NOT NULL,
                        dimension varchar NOT NULL, 
                        resident_cnt int NOT NULL);"""
        clear_table = """TRUNCATE TABLE i_katser_ram_location;"""
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(create, False)
        pg_hook.run(clear_table, False)



    locations = IKatserTopNLocationsOperator(
        task_id='locations',
        top_locations=3,
        dag=dag
    )


    def locations_to_sql(**kwargs):

        task_instance = kwargs['task_instance']
        locations = task_instance.xcom_pull(task_ids='locations')
        #for v in locations:
        #    v = json.loads(v)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for v in locations:
            cursor.execute(f'''insert into i_katser_ram_location 
                                values (%(id)s, %(name)s,%(type)s, %(dimension)s,%(residents)s)''', v)
        conn.commit()
        conn.close()


    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        dag=dag,
        provide_context=True
    )

    locations_to_sql = PythonOperator(
        task_id='locations_to_sql',
        python_callable=locations_to_sql,
        dag=dag,
        provide_context=True
    )

    dummy >> create_table >> locations >> locations_to_sql