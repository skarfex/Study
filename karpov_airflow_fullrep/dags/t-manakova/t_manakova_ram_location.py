import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

from t_manakova_plugins.t_manakova_ram_location import LocationResidentsCountOperator


DEFAULT_ARGS = {
    'owner': 't-manakova',
    'poke_interval': 600,
    'retries': 3,
    'start_date': days_ago(1)
}

with DAG(
    dag_id='t_manakova_ram_location',
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['t-manakova']
) as dag:
    
    get_top_loctions = LocationResidentsCountOperator(
        task_id = 'get_top_loctions',
        top_length = 3
    )
    
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='conn_greenplum_write',
        sql = """
              CREATE TABLE IF NOT EXISTS t_manakova_ram_location (
              id INT NOT NULL,
              name VARCHAR NOT NULL,
              type VARCHAR NOT NULL,
              dimension VARCHAR NOT NULL,
              resident_cnt INT NOT NULL);
              """
    )


    def locations_to_table_func(**kwargs):
        locations = kwargs['ti'].xcom_pull(task_ids='get_top_loctions')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('TRUNCATE TABLE t_manakova_ram_location;')
        conn.commit()
        for location in locations:
            cursor.execute(f'''
                            INSERT INTO t_manakova_ram_location(id, name, type, dimension, resident_cnt) 
                            VALUES (
                                {int(location['id'])}, 
                                '{location['name']}', 
                                '{location['type']}',
                                '{location['dimension']}',
                                {int(location['resident_cnt'])}
                            );
                            '''
                           )
            conn.commit()
        cursor.close()
        conn.close()

    locations_to_table = PythonOperator(
    task_id='locations_to_table_func',
    python_callable=locations_to_table_func)


    def table_to_logs_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM t_manakova_ram_location')
        query_res = cursor.fetchall()
        logging.info('-----------------------------------------------')
        logging.info('Дата исполнения: ' + kwargs['ds'])
        logging.info(query_res)
        logging.info('-----------------------------------------------')
        cursor.close()
        conn.close()
    
    table_to_logs = PythonOperator(
    task_id='table_to_logs',
    python_callable=table_to_logs_func)

    get_top_loctions >> create_table >> locations_to_table >> table_to_logs